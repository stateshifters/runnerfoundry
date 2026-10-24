package sqlite

func sqliteSchemaStatements(extensions ...SchemaExtension) []string {
	statements := append(sqliteJobSchemaStatements(), sqliteCapacitySchemaStatements()...)
	for _, extension := range extensions {
		statements = append(statements, extension.SchemaStatements()...)
	}
	return statements
}

func sqliteJobSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS runnerfoundry_members (
		member_id TEXT NOT NULL PRIMARY KEY CHECK(length(trim(member_id)) > 0),
		heartbeat_at INTEGER NOT NULL
	)`,
		`CREATE TABLE IF NOT EXISTS runnerfoundry_queued_jobs (
		job_id TEXT NOT NULL PRIMARY KEY CHECK(length(job_id) = 36
			AND substr(job_id, 9, 1) = '-' AND substr(job_id, 14, 1) = '-'
			AND substr(job_id, 19, 1) = '-' AND substr(job_id, 24, 1) = '-'
			AND length(replace(job_id, '-', '')) = 32
			AND replace(job_id, '-', '') NOT GLOB '*[^0-9a-f]*'),
		source_name TEXT NOT NULL CHECK(length(trim(source_name)) > 0),
		source_reference TEXT NOT NULL CHECK(length(trim(source_reference)) > 0),
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		enqueued_at INTEGER NOT NULL,
		UNIQUE(source_name, source_reference)
	)`,
		`CREATE INDEX IF NOT EXISTS runnerfoundry_queued_jobs_fifo
		ON runnerfoundry_queued_jobs(runner_pool, enqueued_at)`,
		`CREATE TABLE IF NOT EXISTS runnerfoundry_processing_jobs (
		job_id TEXT NOT NULL PRIMARY KEY,
		source_name TEXT NOT NULL CHECK(length(trim(source_name)) > 0),
		source_reference TEXT NOT NULL CHECK(length(trim(source_reference)) > 0),
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id),
		runner_name TEXT NOT NULL CHECK(length(trim(runner_name)) > 0),
		claimed_at INTEGER NOT NULL,
		inventory_endpoint_id TEXT NOT NULL CHECK(length(trim(inventory_endpoint_id)) > 0),
		runner_started INTEGER NOT NULL DEFAULT 0 CHECK(runner_started IN (0, 1)),
		provisioning_failure_reserved INTEGER NOT NULL DEFAULT 0
			CHECK(provisioning_failure_reserved IN (0, 1)),
		CHECK(NOT (runner_started = 1 AND provisioning_failure_reserved = 1)),
		UNIQUE(source_name, source_reference)
	)`,
		`CREATE INDEX IF NOT EXISTS runnerfoundry_processing_jobs_member_pool
		ON runnerfoundry_processing_jobs(member_id, runner_pool)`,
		`CREATE INDEX IF NOT EXISTS runnerfoundry_processing_jobs_pool
		ON runnerfoundry_processing_jobs(runner_pool)`,
		sqliteProvisioningFailureSchema,
		`CREATE TABLE IF NOT EXISTS runnerfoundry_source_tombstones (
		source_name TEXT NOT NULL CHECK(length(trim(source_name)) > 0),
		source_reference TEXT NOT NULL CHECK(length(trim(source_reference)) > 0),
		PRIMARY KEY(source_name, source_reference)
	)`,
	}
}

const sqliteProvisioningFailureSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_provisioning_failures (
	job_id TEXT NOT NULL PRIMARY KEY
		REFERENCES runnerfoundry_processing_jobs(job_id) ON DELETE RESTRICT,
	runner_name TEXT NOT NULL CHECK(length(trim(runner_name)) > 0),
	is_reported INTEGER NOT NULL DEFAULT 0 CHECK(is_reported IN (0, 1))
)`

func sqliteCapacitySchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS runnerfoundry_member_runner_pools (
		member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id) ON DELETE CASCADE,
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		capacity INTEGER NOT NULL CHECK(capacity > 0),
		PRIMARY KEY(member_id, runner_pool)
	)`,
	}
}

const sqliteSchemaVersion = 109
const sqliteSchemaVersionSQL = "PRAGMA user_version = 109"

const addSQLiteProvisioningFailureFenceSQL = `ALTER TABLE runnerfoundry_processing_jobs
	ADD COLUMN provisioning_failure_reserved INTEGER NOT NULL DEFAULT 0
	CHECK(provisioning_failure_reserved IN (0, 1)
		AND NOT (runner_started = 1 AND provisioning_failure_reserved = 1))`
