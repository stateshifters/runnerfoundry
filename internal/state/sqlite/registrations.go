package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
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

var _ statecontract.RegistrationStore = (*Store)(nil)

func (store *Store) UpsertRegistration(ctx context.Context, registration statecontract.Registration) error {
	return store.executeTransaction(ctx, "upsert registration", upsertRegistrationSQL, registration.RepositoryURL,
		string(registration.RunnerPool), registration.InstallationID, registration.RunnerGroupID, registration.ScaleSetID)
}

func (store *Store) Registration(
	ctx context.Context,
	repositoryURL string,
	runnerPool core.RunnerPool,
) (statecontract.Registration, bool, error) {
	registration, err := scanRegistration(store.executor.QueryRow(ctx, registrationSQL, repositoryURL, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return statecontract.Registration{}, false, nil
	}
	if err != nil {
		return statecontract.Registration{}, false, fmt.Errorf("lookup registration: %w", err)
	}
	return registration, true, nil
}

func (store *Store) Registrations(ctx context.Context) ([]statecontract.Registration, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin registrations lookup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	rows, err := transaction.Query(ctx, registrationsSQL)
	if err != nil {
		return nil, fmt.Errorf("query registrations: %w", err)
	}
	registrations, err := scanRegistrations(rows)
	if err != nil {
		return nil, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit registrations lookup: %w", err)
	}
	return registrations, nil
}

func (store *Store) DeleteRegistration(ctx context.Context, repositoryURL string, runnerPool core.RunnerPool) error {
	return store.executeTransaction(ctx, "delete registration", deleteRegistrationSQL, repositoryURL, string(runnerPool))
}

func scanRegistration(row scanner) (statecontract.Registration, error) {
	var registration statecontract.Registration
	var runnerPool string
	if err := row.Scan(&registration.RepositoryURL, &runnerPool, &registration.InstallationID,
		&registration.RunnerGroupID, &registration.ScaleSetID); err != nil {
		return statecontract.Registration{}, fmt.Errorf("scan registration: %w", err)
	}
	registration.RunnerPool = core.RunnerPool(runnerPool)
	return registration, nil
}

func scanRegistrations(rows Rows) (registrations []statecontract.Registration, resultErr error) {
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
