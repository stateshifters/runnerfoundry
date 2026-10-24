package sqlite

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubtransport "github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	"github.com/stateshifters/runnerfoundry/internal/state"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

const upsertRegistrationSQL = `INSERT INTO runnerfoundry_github_registrations
	(github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES(?, ?, ?, ?, ?, ?)
	ON CONFLICT(repository_url, runner_pool) DO UPDATE SET github_app_id = excluded.github_app_id, installation_id = excluded.installation_id,
		runner_group_id = excluded.runner_group_id, scale_set_id = excluded.scale_set_id`
const registrationsSQL = `SELECT github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM runnerfoundry_github_registrations ORDER BY repository_url, runner_pool`
const deleteActiveRegistrationSQL = `DELETE FROM runnerfoundry_github_registrations
	WHERE github_app_id = ? AND repository_url = ? AND runner_pool = ? AND installation_id = ? AND runner_group_id = ? AND scale_set_id = ?`
const upsertOwnedRegistrationSQL = `INSERT INTO runnerfoundry_github_scale_set_ownership
	(github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id) VALUES(?, ?, ?, ?, ?, ?)
	ON CONFLICT(repository_url, scale_set_id) DO UPDATE SET github_app_id = excluded.github_app_id, runner_pool = excluded.runner_pool,
		installation_id = excluded.installation_id, runner_group_id = excluded.runner_group_id`
const ownedRegistrationsSQL = `SELECT github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
	FROM runnerfoundry_github_scale_set_ownership ORDER BY repository_url, scale_set_id`
const deleteOwnedRegistrationSQL = `DELETE FROM runnerfoundry_github_scale_set_ownership
	WHERE github_app_id = ? AND repository_url = ? AND runner_pool = ? AND installation_id = ? AND runner_group_id = ? AND scale_set_id = ?`

var _ githubsource.RegistrationRepository = (*Store)(nil)

type Store struct {
	transactions   transactionRunner
	memberLiveness state.MemberLivenessReader
}

type transactionRunner interface {
	WithinTransaction(ctx context.Context, operation func(sqlitestate.Transaction) error) error
}

func NewStore(transactions transactionRunner, memberLiveness state.MemberLivenessReader) *Store {
	return &Store{transactions: transactions, memberLiveness: memberLiveness}
}

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
) error {
	arguments := registrationArguments(registration)
	if err := store.transactions.WithinTransaction(ctx, func(transaction sqlitestate.Transaction) error {
		if _, err := transaction.Execute(ctx, activeStatement, arguments...); err != nil {
			return fmt.Errorf("%s active state: %w", operation, err)
		}
		if _, err := transaction.Execute(ctx, ownedStatement, arguments...); err != nil {
			return fmt.Errorf("%s ownership state: %w", operation, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("%s transaction: %w", operation, err)
	}
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

func (store *Store) Registrations(ctx context.Context) ([]githubsource.Registration, error) {
	return store.listRegistrations(ctx, "registrations", registrationsSQL)
}

func (store *Store) OwnedRegistrations(ctx context.Context) ([]githubsource.Registration, error) {
	return store.listRegistrations(ctx, "owned registrations", ownedRegistrationsSQL)
}

func (store *Store) listRegistrations(
	ctx context.Context,
	description, statement string,
) ([]githubsource.Registration, error) {
	var registrations []githubsource.Registration
	if err := store.transactions.WithinTransaction(ctx, func(transaction sqlitestate.Transaction) error {
		return transaction.VisitRows(ctx, statement, func(row sqlitestate.Scanner) error {
			registration, err := scanRegistration(row)
			if err != nil {
				return err
			}
			registrations = append(registrations, registration)
			return nil
		})
	}); err != nil {
		return nil, fmt.Errorf("%s lookup: %w", description, err)
	}
	return registrations, nil
}

func scanRegistration(row sqlitestate.Scanner) (githubsource.Registration, error) {
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

func (store *Store) executeTransaction(
	ctx context.Context,
	description string,
	statement string,
	arguments ...any,
) error {
	if err := store.transactions.WithinTransaction(ctx, func(transaction sqlitestate.Transaction) error {
		if _, err := transaction.Execute(ctx, statement, arguments...); err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("%s transaction: %w", description, err)
	}
	return nil
}
