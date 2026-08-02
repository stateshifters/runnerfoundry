package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func (store *SQLiteDemandStore) StartController(ctx context.Context, appID int64) (result repositorystate.Snapshot, resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin repository controller: %w", err)
	}
	open := true
	defer finishSQLiteTransaction(transaction, &open, "repository controller", &resultErr)
	if _, err = transaction.ExecContext(ctx, `INSERT INTO repository_controls(app_id) VALUES(?)
		ON CONFLICT(app_id) DO NOTHING`, appID); err != nil {
		return result, fmt.Errorf("initialize repository controller: %w", err)
	}
	if _, err = transaction.ExecContext(ctx, `UPDATE repository_controls
		SET controller_epoch = controller_epoch + 1 WHERE app_id = ?`, appID); err != nil {
		return result, fmt.Errorf("start repository controller: %w", err)
	}
	if result, err = loadSQLiteRepositorySnapshot(ctx, transaction, appID); err != nil {
		return repositorystate.Snapshot{}, err
	}
	open = false
	if err = transaction.Commit(); err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("commit repository controller: %w", err)
	}
	return result, nil
}

func (store *SQLiteDemandStore) LoadSnapshot(ctx context.Context, appID int64) (repositorystate.Snapshot, error) {
	return loadSQLiteRepositorySnapshot(ctx, store.database, appID)
}

func (store *SQLiteDemandStore) ApplyDiscovery(
	ctx context.Context,
	appID int64,
	controllerEpoch int64,
	installationIDs []githubapp.InstallationID,
	failedInstallationIDs []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) (result repositorystate.Snapshot, resultErr error) {
	return store.applyRepositoryDiscovery(ctx, sqliteDiscoveryUpdate{
		appID: appID, controllerEpoch: controllerEpoch, installationIDs: installationIDs,
		failedInstallationIDs: failedInstallationIDs, repositories: repositories, completedAt: completedAt,
	})
}

func (store *SQLiteDemandStore) applyRepositoryDiscovery(
	ctx context.Context,
	update sqliteDiscoveryUpdate,
) (result repositorystate.Snapshot, resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin repository discovery: %w", err)
	}
	open := true
	defer finishSQLiteTransaction(transaction, &open, "repository discovery", &resultErr)
	control, err := lockSQLiteRepositoryControl(ctx, transaction, update.appID, update.controllerEpoch)
	if err != nil {
		return result, err
	}
	update.generation = control.DiscoveryGeneration + 1
	if err = applySQLiteDiscovery(ctx, transaction, update); err != nil {
		return result, err
	}
	if result, err = loadSQLiteRepositorySnapshot(ctx, transaction, update.appID); err != nil {
		return repositorystate.Snapshot{}, err
	}
	open = false
	if err = transaction.Commit(); err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("commit repository discovery: %w", err)
	}
	return result, nil
}

type sqliteDiscoveryUpdate struct {
	appID                 int64
	controllerEpoch       int64
	generation            int64
	installationIDs       []githubapp.InstallationID
	failedInstallationIDs []githubapp.InstallationID
	repositories          []repositorystate.Repository
	completedAt           time.Time
}

func applySQLiteDiscovery(ctx context.Context, transaction *sql.Tx, update sqliteDiscoveryUpdate) error {
	if _, err := transaction.ExecContext(ctx, `UPDATE repository_controls SET discovery_generation = ?,
		last_successful_discovery = ?, initialized = 1 WHERE app_id = ? AND controller_epoch = ?`,
		update.generation, update.completedAt.UnixNano(), update.appID, update.controllerEpoch); err != nil {
		return fmt.Errorf("advance repository discovery: %w", err)
	}
	if err := replaceSQLiteInstallations(ctx, transaction, update); err != nil {
		return err
	}
	return replaceSQLiteRepositories(ctx, transaction, update)
}

func replaceSQLiteInstallations(ctx context.Context, transaction *sql.Tx, update sqliteDiscoveryUpdate) error {
	for _, installationID := range update.installationIDs {
		if _, err := transaction.ExecContext(ctx, `INSERT INTO repository_installations(app_id, installation_id, last_seen_generation)
			VALUES(?, ?, ?) ON CONFLICT(app_id, installation_id) DO UPDATE SET last_seen_generation = excluded.last_seen_generation`,
			update.appID, installationID, update.generation); err != nil {
			return fmt.Errorf("record discovered installation: %w", err)
		}
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM repository_installations
		WHERE app_id = ? AND last_seen_generation < ?`, update.appID, update.generation); err != nil {
		return fmt.Errorf("remove undiscovered installations: %w", err)
	}
	return nil
}

func replaceSQLiteRepositories(ctx context.Context, transaction *sql.Tx, update sqliteDiscoveryUpdate) error {
	for _, repository := range update.repositories {
		if _, err := transaction.ExecContext(ctx, `INSERT INTO repositories(app_id, repository_url, installation_id, last_seen_generation)
			VALUES(?, ?, ?, ?) ON CONFLICT(app_id, repository_url) DO UPDATE SET
			installation_id = excluded.installation_id, last_seen_generation = excluded.last_seen_generation`,
			update.appID, repository.URL, repository.InstallationID, update.generation); err != nil {
			return fmt.Errorf("record discovered repository: %w", err)
		}
	}
	failed := make(map[githubapp.InstallationID]struct{}, len(update.failedInstallationIDs))
	for _, installationID := range update.failedInstallationIDs {
		failed[installationID] = struct{}{}
	}
	for _, installationID := range update.installationIDs {
		if _, retain := failed[installationID]; retain {
			continue
		}
		if _, err := transaction.ExecContext(ctx, `DELETE FROM repositories
			WHERE app_id = ? AND installation_id = ? AND last_seen_generation < ?`, update.appID, installationID, update.generation); err != nil {
			return fmt.Errorf("remove undiscovered repositories: %w", err)
		}
	}
	return nil
}

func (store *SQLiteDemandStore) UpsertRegistration(ctx context.Context, appID int64, controllerEpoch int64, registration repositorystate.Registration) (resultErr error) {
	return store.mutateSQLiteRegistration(ctx, appID, controllerEpoch, "upsert repository registration", func(transaction *sql.Tx) error {
		_, err := transaction.ExecContext(ctx, `INSERT INTO repository_registrations(
			app_id, repository_url, scale_set_name, runner_group, github_scale_set_id) VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(app_id, repository_url, scale_set_name) DO UPDATE SET
			runner_group = excluded.runner_group, github_scale_set_id = excluded.github_scale_set_id`,
			appID, registration.RepositoryURL, registration.ScaleSetName, registration.RunnerGroup, registration.GitHubScaleSetID)
		if err != nil {
			return fmt.Errorf("write repository registration: %w", err)
		}
		return nil
	})
}

func (store *SQLiteDemandStore) DeleteRegistration(ctx context.Context, appID int64, controllerEpoch int64, repositoryURL string, scaleSetName string) error {
	return store.mutateSQLiteRegistration(ctx, appID, controllerEpoch, "delete repository registration", func(transaction *sql.Tx) error {
		_, err := transaction.ExecContext(ctx, `DELETE FROM repository_registrations
			WHERE app_id = ? AND repository_url = ? AND scale_set_name = ?`, appID, repositoryURL, scaleSetName)
		if err != nil {
			return fmt.Errorf("remove repository registration: %w", err)
		}
		return nil
	})
}

func (store *SQLiteDemandStore) mutateSQLiteRegistration(ctx context.Context, appID int64, controllerEpoch int64, description string, mutate func(*sql.Tx) error) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s: %w", description, err)
	}
	open := true
	defer finishSQLiteTransaction(transaction, &open, description, &resultErr)
	if _, err = lockSQLiteRepositoryControl(ctx, transaction, appID, controllerEpoch); err != nil {
		return err
	}
	if err = mutate(transaction); err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	open = false
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", description, err)
	}
	return nil
}

func lockSQLiteRepositoryControl(ctx context.Context, queryer interface {
	QueryRowContext(ctx context.Context, statement string, arguments ...any) *sql.Row
}, appID int64, controllerEpoch int64) (repositorystate.Control, error) {
	var control repositorystate.Control
	var lastSuccessfulNanoseconds int64
	err := queryer.QueryRowContext(ctx, `SELECT app_id, discovery_generation, last_successful_discovery,
		controller_epoch, initialized FROM repository_controls WHERE app_id = ? AND controller_epoch = ?`,
		appID, controllerEpoch).Scan(&control.AppID, &control.DiscoveryGeneration, &lastSuccessfulNanoseconds,
		&control.ControllerEpoch, &control.Initialized)
	if errors.Is(err, sql.ErrNoRows) {
		return control, repositorystate.ErrControllerFenced
	}
	if err != nil {
		return control, fmt.Errorf("load repository controller: %w", err)
	}
	if lastSuccessfulNanoseconds != 0 {
		control.LastSuccessfulDiscovery = time.Unix(0, lastSuccessfulNanoseconds).UTC()
	}
	return control, nil
}

type sqliteRepositorySnapshotQueryer interface {
	QueryContext(ctx context.Context, statement string, arguments ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, statement string, arguments ...any) *sql.Row
}

func loadSQLiteRepositorySnapshot(ctx context.Context, queryer sqliteRepositorySnapshotQueryer, appID int64) (repositorystate.Snapshot, error) {
	snapshot, err := loadSQLiteRepositoryControl(ctx, queryer, appID)
	if err != nil || snapshot.Control.AppID == appID && !snapshot.Control.Initialized && snapshot.Control.ControllerEpoch == 0 {
		return snapshot, err
	}
	if snapshot.Repositories, err = loadSQLiteRepositories(ctx, queryer, appID); err != nil {
		return snapshot, err
	}
	if snapshot.Registrations, err = loadSQLiteRegistrations(ctx, queryer, appID); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func loadSQLiteRepositoryControl(ctx context.Context, queryer sqliteRepositorySnapshotQueryer, appID int64) (repositorystate.Snapshot, error) {
	var snapshot repositorystate.Snapshot
	var lastSuccessfulNanoseconds int64
	err := queryer.QueryRowContext(ctx, `SELECT app_id, discovery_generation, last_successful_discovery,
		controller_epoch, initialized FROM repository_controls WHERE app_id = ?`, appID).Scan(
		&snapshot.Control.AppID, &snapshot.Control.DiscoveryGeneration, &lastSuccessfulNanoseconds,
		&snapshot.Control.ControllerEpoch, &snapshot.Control.Initialized)
	if errors.Is(err, sql.ErrNoRows) {
		snapshot.Control.AppID = appID
		return snapshot, nil
	}
	if err != nil {
		return snapshot, fmt.Errorf("load repository control: %w", err)
	}
	if lastSuccessfulNanoseconds != 0 {
		snapshot.Control.LastSuccessfulDiscovery = time.Unix(0, lastSuccessfulNanoseconds).UTC()
	}
	return snapshot, nil
}

func loadSQLiteRepositories(ctx context.Context, queryer sqliteRepositorySnapshotQueryer, appID int64) ([]repositorystate.Repository, error) {
	repositoryRows, err := queryer.QueryContext(ctx, `SELECT app_id, repository_url, installation_id, last_seen_generation
		FROM repositories WHERE app_id = ? ORDER BY repository_url`, appID)
	if err != nil {
		return nil, fmt.Errorf("load repositories: %w", err)
	}
	repositories := make([]repositorystate.Repository, 0)
	for repositoryRows.Next() {
		var repository repositorystate.Repository
		if err = repositoryRows.Scan(&repository.AppID, &repository.URL, &repository.InstallationID, &repository.LastSeenGeneration); err != nil {
			return nil, errors.Join(fmt.Errorf("scan repository: %w", err), closeSQLiteRows(repositoryRows, "repository rows"))
		}
		repositories = append(repositories, repository)
	}
	if err = errors.Join(repositoryRows.Err(), closeSQLiteRows(repositoryRows, "repository rows")); err != nil {
		return nil, fmt.Errorf("read repositories: %w", err)
	}
	return repositories, nil
}

func loadSQLiteRegistrations(ctx context.Context, queryer sqliteRepositorySnapshotQueryer, appID int64) ([]repositorystate.Registration, error) {
	registrationRows, err := queryer.QueryContext(ctx, `SELECT app_id, repository_url, scale_set_name, runner_group, github_scale_set_id
		FROM repository_registrations WHERE app_id = ? ORDER BY repository_url, scale_set_name`, appID)
	if err != nil {
		return nil, fmt.Errorf("load repository registrations: %w", err)
	}
	registrations := make([]repositorystate.Registration, 0)
	for registrationRows.Next() {
		var registration repositorystate.Registration
		if err = registrationRows.Scan(&registration.AppID, &registration.RepositoryURL, &registration.ScaleSetName,
			&registration.RunnerGroup, &registration.GitHubScaleSetID); err != nil {
			return nil, errors.Join(fmt.Errorf("scan repository registration: %w", err), closeSQLiteRows(registrationRows, "registration rows"))
		}
		registrations = append(registrations, registration)
	}
	if err = errors.Join(registrationRows.Err(), closeSQLiteRows(registrationRows, "registration rows")); err != nil {
		return nil, fmt.Errorf("read repository registrations: %w", err)
	}
	return registrations, nil
}

func closeSQLiteRows(rows *sql.Rows, description string) error {
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close %s: %w", description, err)
	}
	return nil
}
