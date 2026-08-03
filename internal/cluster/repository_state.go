package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func (store *Store) StartController(ctx context.Context, appID int64) (result repositorystate.Snapshot, resultErr error) {
	connection, transaction, err := store.beginRepositoryStateTransaction(ctx, "repository controller")
	if err != nil {
		return result, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	open := true
	defer finishTransaction(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Exec(ctx, `INSERT INTO scaler_repository_controls(app_id) VALUES($1)
		ON CONFLICT(app_id) DO NOTHING`, appID); err != nil {
		return result, fmt.Errorf("initialize repository controller: %w", err)
	}
	if _, err = transaction.Exec(ctx, `UPDATE scaler_repository_controls SET controller_epoch = controller_epoch + 1
		WHERE app_id = $1`, appID); err != nil {
		return result, fmt.Errorf("start repository controller: %w", err)
	}
	if result, err = loadPostgreSQLRepositorySnapshot(ctx, transaction, appID); err != nil {
		return repositorystate.Snapshot{}, err
	}
	open = false
	if err = transaction.Commit(ctx); err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("commit repository controller: %w", err)
	}
	return result, nil
}

func (store *Store) LoadSnapshot(ctx context.Context, appID int64) (result repositorystate.Snapshot, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return result, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	return loadPostgreSQLRepositorySnapshot(ctx, connection, appID)
}

func (store *Store) ApplyDiscovery(
	ctx context.Context,
	appID int64,
	controllerEpoch int64,
	installationIDs []githubapp.InstallationID,
	failedInstallationIDs []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) (result repositorystate.Snapshot, resultErr error) {
	return store.applyRepositoryDiscovery(ctx, postgreSQLDiscoveryUpdate{
		appID: appID, controllerEpoch: controllerEpoch, installationIDs: installationIDs,
		failedInstallationIDs: failedInstallationIDs, repositories: repositories, completedAt: completedAt,
	})
}

func (store *Store) applyRepositoryDiscovery(
	ctx context.Context,
	update postgreSQLDiscoveryUpdate,
) (result repositorystate.Snapshot, resultErr error) {
	connection, transaction, err := store.beginRepositoryStateTransaction(ctx, "repository discovery")
	if err != nil {
		return result, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	open := true
	defer finishTransaction(ctx, transaction, &open, &resultErr)
	control, err := lockPostgreSQLRepositoryControl(ctx, transaction, update.appID, update.controllerEpoch)
	if err != nil {
		return result, err
	}
	update.generation = control.DiscoveryGeneration + 1
	if err = applyPostgreSQLDiscovery(ctx, transaction, update); err != nil {
		return result, err
	}
	if result, err = loadPostgreSQLRepositorySnapshot(ctx, transaction, update.appID); err != nil {
		return repositorystate.Snapshot{}, err
	}
	open = false
	if err = transaction.Commit(ctx); err != nil {
		return repositorystate.Snapshot{}, fmt.Errorf("commit repository discovery: %w", err)
	}
	return result, nil
}

type postgreSQLDiscoveryUpdate struct {
	appID                 int64
	controllerEpoch       int64
	generation            int64
	installationIDs       []githubapp.InstallationID
	failedInstallationIDs []githubapp.InstallationID
	repositories          []repositorystate.Repository
	completedAt           time.Time
}

func applyPostgreSQLDiscovery(ctx context.Context, transaction *databaseTransaction, update postgreSQLDiscoveryUpdate) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_repository_controls SET discovery_generation = $1,
		last_successful_discovery = $2, initialized = true WHERE app_id = $3 AND controller_epoch = $4`,
		update.generation, update.completedAt, update.appID, update.controllerEpoch); err != nil {
		return fmt.Errorf("advance repository discovery: %w", err)
	}
	if err := replacePostgreSQLInstallations(ctx, transaction, update); err != nil {
		return err
	}
	if err := replacePostgreSQLRepositories(ctx, transaction, update); err != nil {
		return err
	}
	return nil
}

func replacePostgreSQLInstallations(ctx context.Context, transaction *databaseTransaction, update postgreSQLDiscoveryUpdate) error {
	for _, installationID := range update.installationIDs {
		if _, err := transaction.Exec(ctx, `INSERT INTO scaler_installations(app_id, installation_id, last_seen_generation)
			VALUES($1, $2, $3) ON CONFLICT(app_id, installation_id) DO UPDATE
			SET last_seen_generation = EXCLUDED.last_seen_generation`, update.appID, installationID, update.generation); err != nil {
			return fmt.Errorf("record discovered installation: %w", err)
		}
	}
	if err := withdrawRemovedInstallationAssignedObservations(ctx, transaction, update); err != nil {
		return err
	}
	if _, err := transaction.Exec(ctx, `DELETE FROM scaler_installations
		WHERE app_id = $1 AND last_seen_generation < $2`, update.appID, update.generation); err != nil {
		return fmt.Errorf("remove undiscovered installations: %w", err)
	}
	return nil
}

func replacePostgreSQLRepositories(ctx context.Context, transaction *databaseTransaction, update postgreSQLDiscoveryUpdate) error {
	if err := recordDiscoveredRepositories(ctx, transaction, update); err != nil {
		return err
	}
	return removeAuthoritativelyMissingRepositories(ctx, transaction, update)
}

func recordDiscoveredRepositories(ctx context.Context, transaction *databaseTransaction, update postgreSQLDiscoveryUpdate) error {
	for _, repository := range update.repositories {
		if _, err := transaction.Exec(ctx, `INSERT INTO scaler_repositories(app_id, repository_url, installation_id, last_seen_generation)
			VALUES($1, $2, $3, $4) ON CONFLICT(app_id, repository_url) DO UPDATE SET
			installation_id = EXCLUDED.installation_id, last_seen_generation = EXCLUDED.last_seen_generation`,
			update.appID, repository.URL, repository.InstallationID, update.generation); err != nil {
			return fmt.Errorf("record discovered repository: %w", err)
		}
	}
	return nil
}

func removeAuthoritativelyMissingRepositories(
	ctx context.Context,
	transaction *databaseTransaction,
	update postgreSQLDiscoveryUpdate,
) error {
	failed := make(map[githubapp.InstallationID]struct{}, len(update.failedInstallationIDs))
	for _, installationID := range update.failedInstallationIDs {
		failed[installationID] = struct{}{}
	}
	for _, installationID := range update.installationIDs {
		if _, retain := failed[installationID]; retain {
			continue
		}
		if err := withdrawUndiscoveredAssignedObservations(
			ctx, transaction, update.appID, installationID, update.generation, update.completedAt,
		); err != nil {
			return err
		}
		if _, err := transaction.Exec(ctx, `DELETE FROM scaler_repositories
			WHERE app_id = $1 AND installation_id = $2 AND last_seen_generation < $3`,
			update.appID, installationID, update.generation); err != nil {
			return fmt.Errorf("remove undiscovered repositories: %w", err)
		}
	}
	return nil
}

func (store *Store) UpsertRegistration(ctx context.Context, appID int64, controllerEpoch int64, registration repositorystate.Registration) error {
	return store.mutatePostgreSQLRegistration(ctx, appID, controllerEpoch, "upsert repository registration", func(transaction *databaseTransaction) error {
		_, err := transaction.Exec(ctx, `INSERT INTO scaler_registrations(
			app_id, repository_url, scale_set_name, runner_group, github_scale_set_id) VALUES($1, $2, $3, $4, $5)
			ON CONFLICT(app_id, repository_url, scale_set_name) DO UPDATE SET
			runner_group = EXCLUDED.runner_group, github_scale_set_id = EXCLUDED.github_scale_set_id`,
			appID, registration.RepositoryURL, registration.ScaleSetName, registration.RunnerGroup, registration.GitHubScaleSetID)
		return err
	})
}

func (store *Store) DeleteRegistration(ctx context.Context, appID int64, controllerEpoch int64, repositoryURL string, scaleSetName string) error {
	return store.mutatePostgreSQLRegistration(ctx, appID, controllerEpoch, "delete repository registration", func(transaction *databaseTransaction) error {
		if err := withdrawRegistrationAssignedObservation(
			ctx, transaction, appID, repositoryURL, scaleSetName, time.Now().UTC(),
		); err != nil {
			return err
		}
		_, err := transaction.Exec(ctx, `DELETE FROM scaler_registrations
			WHERE app_id = $1 AND repository_url = $2 AND scale_set_name = $3`, appID, repositoryURL, scaleSetName)
		return err
	})
}

func (store *Store) mutatePostgreSQLRegistration(ctx context.Context, appID int64, controllerEpoch int64, description string, mutate func(*databaseTransaction) error) (resultErr error) {
	connection, transaction, err := store.beginRepositoryStateTransaction(ctx, description)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	open := true
	defer finishTransaction(ctx, transaction, &open, &resultErr)
	if _, err = lockPostgreSQLRepositoryControl(ctx, transaction, appID, controllerEpoch); err != nil {
		return err
	}
	if err = mutate(transaction); err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	open = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", description, err)
	}
	return nil
}

func (store *Store) beginRepositoryStateTransaction(ctx context.Context, description string) (*databaseConnection, *databaseTransaction, error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, nil, err
	}
	transaction, err := connection.Begin(ctx)
	if err != nil {
		closeErr := connection.Close(context.WithoutCancel(ctx))
		return nil, nil, errors.Join(fmt.Errorf("begin %s: %w", description, err), closeErr)
	}
	return connection, transaction, nil
}

func lockPostgreSQLRepositoryControl(ctx context.Context, queryer interface {
	QueryRow(ctx context.Context, statement string, arguments ...any) databaseRow
}, appID int64, controllerEpoch int64) (repositorystate.Control, error) {
	var control repositorystate.Control
	var lastSuccessful *time.Time
	err := queryer.QueryRow(ctx, `SELECT app_id, discovery_generation, last_successful_discovery,
		controller_epoch, initialized FROM scaler_repository_controls
		WHERE app_id = $1 AND controller_epoch = $2 FOR UPDATE`, appID, controllerEpoch).Scan(
		&control.AppID, &control.DiscoveryGeneration, &lastSuccessful, &control.ControllerEpoch, &control.Initialized)
	if errors.Is(err, pgx.ErrNoRows) {
		return control, repositorystate.ErrControllerFenced
	}
	if err != nil {
		return control, fmt.Errorf("load repository controller: %w", err)
	}
	if lastSuccessful != nil {
		control.LastSuccessfulDiscovery = lastSuccessful.UTC()
	}
	return control, nil
}

type postgreSQLRepositorySnapshotQueryer interface {
	Query(ctx context.Context, statement string, arguments ...any) (*databaseRows, error)
	QueryRow(ctx context.Context, statement string, arguments ...any) databaseRow
}

func loadPostgreSQLRepositorySnapshot(ctx context.Context, queryer postgreSQLRepositorySnapshotQueryer, appID int64) (repositorystate.Snapshot, error) {
	snapshot, err := loadPostgreSQLRepositoryControl(ctx, queryer, appID)
	if err != nil || snapshot.Control.AppID == appID && !snapshot.Control.Initialized && snapshot.Control.ControllerEpoch == 0 {
		return snapshot, err
	}
	if snapshot.Repositories, err = loadPostgreSQLRepositories(ctx, queryer, appID); err != nil {
		return snapshot, err
	}
	if snapshot.Registrations, err = loadPostgreSQLRegistrations(ctx, queryer, appID); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func loadPostgreSQLRepositoryControl(ctx context.Context, queryer postgreSQLRepositorySnapshotQueryer, appID int64) (repositorystate.Snapshot, error) {
	var snapshot repositorystate.Snapshot
	var lastSuccessful *time.Time
	err := queryer.QueryRow(ctx, `SELECT app_id, discovery_generation, last_successful_discovery,
		controller_epoch, initialized FROM scaler_repository_controls WHERE app_id = $1`, appID).Scan(
		&snapshot.Control.AppID, &snapshot.Control.DiscoveryGeneration, &lastSuccessful,
		&snapshot.Control.ControllerEpoch, &snapshot.Control.Initialized)
	if errors.Is(err, pgx.ErrNoRows) {
		snapshot.Control.AppID = appID
		return snapshot, nil
	}
	if err != nil {
		return snapshot, fmt.Errorf("load repository control: %w", err)
	}
	if lastSuccessful != nil {
		snapshot.Control.LastSuccessfulDiscovery = lastSuccessful.UTC()
	}
	return snapshot, nil
}

func loadPostgreSQLRepositories(ctx context.Context, queryer postgreSQLRepositorySnapshotQueryer, appID int64) ([]repositorystate.Repository, error) {
	repositoryRows, err := queryer.Query(ctx, `SELECT app_id, repository_url, installation_id, last_seen_generation
		FROM scaler_repositories WHERE app_id = $1 ORDER BY repository_url`, appID)
	if err != nil {
		return nil, fmt.Errorf("load repositories: %w", err)
	}
	repositories := make([]repositorystate.Repository, 0)
	for repositoryRows.Next() {
		var repository repositorystate.Repository
		if err = repositoryRows.Scan(&repository.AppID, &repository.URL, &repository.InstallationID, &repository.LastSeenGeneration); err != nil {
			repositoryRows.Close()
			return nil, fmt.Errorf("scan repository: %w", err)
		}
		repositories = append(repositories, repository)
	}
	repositoryRows.Close()
	if err = repositoryRows.Err(); err != nil {
		return nil, fmt.Errorf("read repositories: %w", err)
	}
	return repositories, nil
}

func loadPostgreSQLRegistrations(ctx context.Context, queryer postgreSQLRepositorySnapshotQueryer, appID int64) ([]repositorystate.Registration, error) {
	registrationRows, err := queryer.Query(ctx, `SELECT app_id, repository_url, scale_set_name, runner_group, github_scale_set_id
		FROM scaler_registrations WHERE app_id = $1 ORDER BY repository_url, scale_set_name`, appID)
	if err != nil {
		return nil, fmt.Errorf("load repository registrations: %w", err)
	}
	registrations := make([]repositorystate.Registration, 0)
	for registrationRows.Next() {
		var registration repositorystate.Registration
		if err = registrationRows.Scan(&registration.AppID, &registration.RepositoryURL, &registration.ScaleSetName,
			&registration.RunnerGroup, &registration.GitHubScaleSetID); err != nil {
			registrationRows.Close()
			return nil, fmt.Errorf("scan repository registration: %w", err)
		}
		registrations = append(registrations, registration)
	}
	registrationRows.Close()
	if err = registrationRows.Err(); err != nil {
		return nil, fmt.Errorf("read repository registrations: %w", err)
	}
	return registrations, nil
}
