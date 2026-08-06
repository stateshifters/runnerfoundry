package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

const upsertRegistrationSQL = `INSERT INTO public.runnerfoundry_github_registrations
	(repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT(repository_url, runner_pool) DO UPDATE SET installation_id = excluded.installation_id,
		runner_group_id = excluded.runner_group_id, scale_set_id = excluded.scale_set_id`
const registrationSQL = `SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM public.runnerfoundry_github_registrations WHERE repository_url = $1 AND runner_pool = $2`
const registrationsSQL = `SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM public.runnerfoundry_github_registrations ORDER BY repository_url, runner_pool`
const deleteRegistrationSQL = `DELETE FROM public.runnerfoundry_github_registrations WHERE repository_url = $1 AND runner_pool = $2`
const deleteAllRegistrationsSQL = `DELETE FROM public.runnerfoundry_github_registrations`

var _ githubsource.RegistrationRepository = (*Store)(nil)

func (store *Store) UpsertRegistration(ctx context.Context, registration githubsource.Registration) error {
	return store.executeTransaction(ctx, "upsert registration", upsertRegistrationSQL, registration.RepositoryURL,
		string(registration.RunnerPool), registration.InstallationID, registration.RunnerGroupID, registration.ScaleSetID)
}

func (store *Store) ReplaceRegistrations(
	ctx context.Context,
	registrations []githubsource.Registration,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin registration replacement: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Execute(ctx, deleteAllRegistrationsSQL); err != nil {
		return fmt.Errorf("clear registration snapshot: %w", err)
	}
	for _, registration := range registrations {
		if _, err = transaction.Execute(ctx, upsertRegistrationSQL, registration.RepositoryURL,
			string(registration.RunnerPool), registration.InstallationID, registration.RunnerGroupID,
			registration.ScaleSetID); err != nil {
			return fmt.Errorf("insert registration snapshot: %w", err)
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit registration replacement: %w", err)
	}
	open = false
	return nil
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
	if err := row.Scan(&registration.RepositoryURL, &runnerPool, &registration.InstallationID,
		&registration.RunnerGroupID, &registration.ScaleSetID); err != nil {
		return githubsource.Registration{}, fmt.Errorf("scan registration: %w", err)
	}
	registration.RunnerPool = core.RunnerPool(runnerPool)
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
