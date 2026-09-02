package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubtransport "github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

const upsertRegistrationSQL = `INSERT INTO public.runnerfoundry_github_registrations
	(github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES($1, $2, $3, $4, $5, $6)
	ON CONFLICT(repository_url, runner_pool) DO UPDATE SET github_app_id = excluded.github_app_id, installation_id = excluded.installation_id,
		runner_group_id = excluded.runner_group_id, scale_set_id = excluded.scale_set_id`
const registrationsSQL = `SELECT github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM public.runnerfoundry_github_registrations ORDER BY repository_url, runner_pool`
const deleteActiveRegistrationSQL = `DELETE FROM public.runnerfoundry_github_registrations
	WHERE github_app_id = $1 AND repository_url = $2 AND runner_pool = $3 AND installation_id = $4 AND runner_group_id = $5 AND scale_set_id = $6`
const upsertOwnedRegistrationSQL = `INSERT INTO public.runnerfoundry_github_scale_set_ownership
	(github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES($1, $2, $3, $4, $5, $6)
	ON CONFLICT(repository_url, scale_set_id) DO UPDATE SET github_app_id = excluded.github_app_id, runner_pool = excluded.runner_pool,
		installation_id = excluded.installation_id, runner_group_id = excluded.runner_group_id`
const ownedRegistrationsSQL = `SELECT github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM public.runnerfoundry_github_scale_set_ownership ORDER BY repository_url, scale_set_id`
const deleteOwnedRegistrationSQL = `DELETE FROM public.runnerfoundry_github_scale_set_ownership
	WHERE github_app_id = $1 AND repository_url = $2 AND runner_pool = $3 AND installation_id = $4 AND runner_group_id = $5 AND scale_set_id = $6`

var _ githubsource.RegistrationRepository = (*Store)(nil)

type Store struct{ executor postgresstate.Executor }

func NewStore(connection postgresstate.Connection) *Store {
	return newExecutorStore(connection.Executor())
}

func newExecutorStore(executor postgresstate.Executor) *Store { return &Store{executor: executor} }

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
	return []any{registration.GitHubAppID, registration.RepositoryURL, string(registration.RunnerPool), registration.InstallationID,
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

func scanRegistration(row scanner) (githubsource.Registration, error) {
	var registration githubsource.Registration
	var runnerPool string
	var installationID, runnerGroupID, scaleSetID int64
	var githubAppID int64
	if err := row.Scan(&githubAppID, &registration.RepositoryURL, &runnerPool, &installationID,
		&runnerGroupID, &scaleSetID); err != nil {
		return githubsource.Registration{}, fmt.Errorf("scan registration: %w", err)
	}
	registration.RunnerPool = core.RunnerPool(runnerPool)
	registration.GitHubAppID = githubsource.AppID(githubAppID)
	registration.InstallationID = githubtransport.InstallationID(installationID)
	registration.RunnerGroupID = githubtransport.RunnerGroupID(runnerGroupID)
	registration.ScaleSetID = githubtransport.ScaleSetID(scaleSetID)
	return registration, nil
}

type scanner interface {
	Scan(destinations ...any) error
}

func scanRegistrations(rows postgresstate.Rows) (registrations []githubsource.Registration, resultErr error) {
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

func rollback(ctx context.Context, transaction postgresstate.Transaction, open *bool, resultErr *error) {
	if *open {
		*resultErr = errors.Join(*resultErr, transaction.Rollback(ctx))
	}
}

func (store *Store) executeTransaction(
	ctx context.Context,
	description string,
	statement string,
	arguments ...any,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Execute(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", description, err)
	}
	open = false
	return nil
}
