package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	githubtransport "github.com/meza/RunnerFoundry/internal/source/github/transport"
)

const upsertRegistrationSQL = `INSERT INTO runnerfoundry_github_registrations
	(repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES(?, ?, ?, ?, ?)
	ON CONFLICT(repository_url, runner_pool) DO UPDATE SET installation_id = excluded.installation_id,
		runner_group_id = excluded.runner_group_id, scale_set_id = excluded.scale_set_id`
const registrationSQL = `SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM runnerfoundry_github_registrations WHERE repository_url = ? AND runner_pool = ?`
const registrationsSQL = `SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM runnerfoundry_github_registrations ORDER BY repository_url, runner_pool`
const deleteRegistrationSQL = `DELETE FROM runnerfoundry_github_registrations WHERE repository_url = ? AND runner_pool = ?`
const deleteAllRegistrationsSQL = `DELETE FROM runnerfoundry_github_registrations`
const upsertOwnedRegistrationSQL = `INSERT INTO runnerfoundry_github_scale_set_ownership
	(repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES(?, ?, ?, ?, ?)
	ON CONFLICT(repository_url, scale_set_id) DO UPDATE SET runner_pool = excluded.runner_pool,
		installation_id = excluded.installation_id, runner_group_id = excluded.runner_group_id`
const ownedRegistrationsSQL = `SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM runnerfoundry_github_scale_set_ownership ORDER BY repository_url, scale_set_id`
const deleteAllOwnedRegistrationsSQL = `DELETE FROM runnerfoundry_github_scale_set_ownership`

var _ githubsource.RegistrationRepository = (*Store)(nil)

func (store *Store) UpsertRegistration(ctx context.Context, registration githubsource.Registration) error {
	return store.executeTransaction(ctx, "upsert registration", upsertRegistrationSQL, registration.RepositoryURL,
		string(registration.RunnerPool), registration.InstallationID, registration.RunnerGroupID, registration.ScaleSetID)
}

func (store *Store) ReplaceRegistrationSnapshot(
	ctx context.Context,
	snapshot githubsource.RegistrationSnapshot,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin registration snapshot replacement: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Execute(ctx, deleteAllRegistrationsSQL); err != nil {
		return fmt.Errorf("clear active registration snapshot: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteAllOwnedRegistrationsSQL); err != nil {
		return fmt.Errorf("clear owned registration snapshot: %w", err)
	}
	if err = insertRegistrationSnapshot(ctx, transaction, upsertRegistrationSQL, snapshot.Active); err != nil {
		return fmt.Errorf("insert active registration snapshot: %w", err)
	}
	if err = insertRegistrationSnapshot(ctx, transaction, upsertOwnedRegistrationSQL, snapshot.Owned); err != nil {
		return fmt.Errorf("insert owned registration snapshot: %w", err)
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit registration snapshot replacement: %w", err)
	}
	open = false
	return nil
}

func insertRegistrationSnapshot(
	ctx context.Context,
	transaction Transaction,
	statement string,
	registrations []githubsource.Registration,
) error {
	for _, registration := range registrations {
		if _, err := transaction.Execute(ctx, statement, registration.RepositoryURL, string(registration.RunnerPool),
			registration.InstallationID, registration.RunnerGroupID, registration.ScaleSetID); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) RegistrationSnapshot(
	ctx context.Context,
) (_ githubsource.RegistrationSnapshot, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return githubsource.RegistrationSnapshot{}, fmt.Errorf("begin registration snapshot lookup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	active, err := queryRegistrations(ctx, transaction, registrationsSQL)
	if err != nil {
		return githubsource.RegistrationSnapshot{}, fmt.Errorf("query active registrations: %w", err)
	}
	owned, err := queryRegistrations(ctx, transaction, ownedRegistrationsSQL)
	if err != nil {
		return githubsource.RegistrationSnapshot{}, fmt.Errorf("query owned registrations: %w", err)
	}
	if err = transaction.Commit(ctx); err != nil {
		return githubsource.RegistrationSnapshot{}, fmt.Errorf("commit registration snapshot lookup: %w", err)
	}
	open = false
	return githubsource.RegistrationSnapshot{Active: active, Owned: owned}, nil
}

func queryRegistrations(ctx context.Context, transaction Transaction, statement string) ([]githubsource.Registration, error) {
	rows, err := transaction.Query(ctx, statement)
	if err != nil {
		return nil, err
	}
	return scanRegistrations(rows)
}

func (store *Store) Registration(
	ctx context.Context,
	repositoryURL string,
	runnerPool core.RunnerPool,
) (githubsource.Registration, bool, error) {
	registration, err := scanRegistration(store.executor.QueryRow(ctx, registrationSQL, repositoryURL, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return githubsource.Registration{}, false, nil
	}
	if err != nil {
		return githubsource.Registration{}, false, fmt.Errorf("lookup registration: %w", err)
	}
	return registration, true, nil
}

func (store *Store) Registrations(ctx context.Context) (_ []githubsource.Registration, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin registrations lookup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	rows, err := transaction.Query(ctx, registrationsSQL)
	if err != nil {
		return nil, fmt.Errorf("query registrations: %w", err)
	}
	registrations, err := scanRegistrations(rows)
	if err != nil {
		return nil, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit registrations lookup: %w", err)
	}
	open = false
	return registrations, nil
}

func (store *Store) DeleteRegistration(ctx context.Context, repositoryURL string, runnerPool core.RunnerPool) error {
	return store.executeTransaction(ctx, "delete registration", deleteRegistrationSQL, repositoryURL, string(runnerPool))
}

func scanRegistration(row scanner) (githubsource.Registration, error) {
	var registration githubsource.Registration
	var runnerPool string
	var installationID, runnerGroupID, scaleSetID int64
	if err := row.Scan(&registration.RepositoryURL, &runnerPool, &installationID,
		&runnerGroupID, &scaleSetID); err != nil {
		return githubsource.Registration{}, fmt.Errorf("scan registration: %w", err)
	}
	registration.RunnerPool = core.RunnerPool(runnerPool)
	registration.InstallationID = githubtransport.InstallationID(installationID)
	registration.RunnerGroupID = githubtransport.RunnerGroupID(runnerGroupID)
	registration.ScaleSetID = githubtransport.ScaleSetID(scaleSetID)
	return registration, nil
}

func scanRegistrations(rows Rows) (registrations []githubsource.Registration, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		registration, err := scanRegistration(rows)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate registrations: %w", err)
	}
	return registrations, nil
}
