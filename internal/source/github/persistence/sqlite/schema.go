package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type SchemaExtension struct{}

func Schema() SchemaExtension { return SchemaExtension{} }

func (SchemaExtension) SchemaStatements() []string {
	return []string{sqliteGitHubRegistrationsSchema, sqliteGitHubMemberRunnerPoolsSchema, sqliteGitHubOwnershipSchema}
}

func (SchemaExtension) Upgrade(ctx context.Context, transaction *sql.Tx, version int) (int, bool, error) {
	switch version {
	case 106:
		if _, err := transaction.ExecContext(ctx, sqliteGitHubMemberRunnerPoolsSchema); err != nil {
			return version, true, fmt.Errorf("add SQLite GitHub Runner Pool declarations: %w", err)
		}
		if err := replaceSQLiteGitHubAdapterState(ctx, transaction); err != nil {
			return version, true, err
		}
		return 108, true, nil
	case 107:
		if err := replaceSQLiteGitHubAdapterState(ctx, transaction); err != nil {
			return version, true, err
		}
		return 108, true, nil
	default:
		return version, false, nil
	}
}

func replaceSQLiteGitHubAdapterState(ctx context.Context, transaction *sql.Tx) error {
	for _, statement := range []string{
		`DROP TABLE runnerfoundry_github_registrations`,
		`DROP TABLE runnerfoundry_github_scale_set_ownership`,
		sqliteGitHubRegistrationsSchema,
		sqliteGitHubOwnershipSchema,
	} {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("replace SQLite GitHub App state: %w", err)
		}
	}
	return nil
}

const sqliteGitHubRegistrationsSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_github_registrations (
	github_app_id INTEGER NOT NULL CHECK(github_app_id > 0),
	repository_url TEXT NOT NULL CHECK(length(trim(repository_url)) > 0),
	runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
	installation_id INTEGER NOT NULL CHECK(installation_id > 0),
	runner_group_id INTEGER NOT NULL CHECK(runner_group_id > 0),
	scale_set_id INTEGER NOT NULL CHECK(scale_set_id > 0),
	PRIMARY KEY(repository_url, runner_pool)
)`

const sqliteGitHubOwnershipSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_github_scale_set_ownership (
	github_app_id INTEGER NOT NULL CHECK(github_app_id > 0),
	repository_url TEXT NOT NULL CHECK(length(trim(repository_url)) > 0),
	runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
	installation_id INTEGER NOT NULL CHECK(installation_id > 0),
	runner_group_id INTEGER NOT NULL CHECK(runner_group_id > 0),
	scale_set_id INTEGER NOT NULL CHECK(scale_set_id > 0),
	PRIMARY KEY(repository_url, scale_set_id)
)`

const sqliteGitHubMemberRunnerPoolsSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_github_member_runner_pools (
	member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id) ON DELETE CASCADE,
	runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
	runner_group TEXT NOT NULL CHECK(length(trim(runner_group)) > 0),
	PRIMARY KEY(member_id, runner_pool)
)`
