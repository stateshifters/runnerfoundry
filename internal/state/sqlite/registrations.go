package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubtransport "github.com/stateshifters/runnerfoundry/internal/source/github/transport"
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
const deleteActiveRegistrationSQL = `DELETE FROM runnerfoundry_github_registrations
	WHERE repository_url = ? AND runner_pool = ? AND installation_id = ? AND runner_group_id = ? AND scale_set_id = ?`
const upsertOwnedRegistrationSQL = `INSERT INTO runnerfoundry_github_scale_set_ownership
	(repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES(?, ?, ?, ?, ?)
	ON CONFLICT(repository_url, scale_set_id) DO UPDATE SET runner_pool = excluded.runner_pool,
		installation_id = excluded.installation_id, runner_group_id = excluded.runner_group_id`
const ownedRegistrationsSQL = `SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM runnerfoundry_github_scale_set_ownership ORDER BY repository_url, scale_set_id`
const deleteOwnedRegistrationSQL = `DELETE FROM runnerfoundry_github_scale_set_ownership
	WHERE repository_url = ? AND runner_pool = ? AND installation_id = ? AND runner_group_id = ? AND scale_set_id = ?`

var _ githubsource.RegistrationRepository = (*Store)(nil)

func (store *Store) UpsertRegistration(
	ctx context.Context,
	registration githubsource.Registration,
) error {
	return store.mutateRegistrationPair(ctx, "upsert registration", upsertRegistrationSQL,
		upsertOwnedRegistrationSQL, registration)
}

func (store *Store) mutateRegistrationPair(
	ctx context.Context,
	operation, activeStatement, ownedStatement string,
	registration githubsource.Registration,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", operation, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	arguments := registrationArguments(registration)
	if _, err = transaction.Execute(ctx, activeStatement, arguments...); err != nil {
		return fmt.Errorf("%s active state: %w", operation, err)
	}
	if _, err = transaction.Execute(ctx, ownedStatement, arguments...); err != nil {
		return fmt.Errorf("%s ownership state: %w", operation, err)
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", operation, err)
	}
	open = false
	return nil
}

func registrationArguments(registration githubsource.Registration) []any {
	return []any{registration.RepositoryURL, string(registration.RunnerPool), registration.InstallationID,
		registration.RunnerGroupID, registration.ScaleSetID}
}

func (store *Store) DeleteRegistrationState(
	ctx context.Context,
	registration githubsource.Registration,
) error {
	return store.mutateRegistrationPair(ctx, "delete registration", deleteActiveRegistrationSQL,
		deleteOwnedRegistrationSQL, registration)
}

func (store *Store) DeleteActiveRegistration(ctx context.Context, registration githubsource.Registration) error {
	return store.executeTransaction(ctx, "delete active registration", deleteActiveRegistrationSQL,
		registrationArguments(registration)...)
}

func (store *Store) DeleteOwnedRegistration(ctx context.Context, registration githubsource.Registration) error {
	return store.executeTransaction(ctx, "delete owned registration", deleteOwnedRegistrationSQL,
		registrationArguments(registration)...)
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
	return store.listRegistrations(ctx, "registrations", registrationsSQL)
}

func (store *Store) OwnedRegistrations(ctx context.Context) (_ []githubsource.Registration, resultErr error) {
	return store.listRegistrations(ctx, "owned registrations", ownedRegistrationsSQL)
}

func (store *Store) listRegistrations(
	ctx context.Context,
	description, statement string,
) (_ []githubsource.Registration, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin %s lookup: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	rows, err := transaction.Query(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", description, err)
	}
	registrations, err := scanRegistrations(rows)
	if err != nil {
		return nil, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit %s lookup: %w", description, err)
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
