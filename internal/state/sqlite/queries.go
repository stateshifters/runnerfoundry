package sqlite

const queuedJobColumns = `job_id, source_name, source_reference, runner_pool, enqueued_at`
const processingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_name, runner_started, EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal,
	CASE WHEN EXISTS (SELECT 1 FROM runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id AND failures.is_reported = 1) THEN 'reported'
		WHEN provisioning_failure_reserved = 1 THEN 'reporting' ELSE '' END AS provisioning_failure_state`
const claimedProcessingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_name, claimed_at, inventory_endpoint_id, runner_started,
	EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal,
	CASE WHEN EXISTS (SELECT 1 FROM runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id AND failures.is_reported = 1) THEN 'reported'
		WHEN provisioning_failure_reserved = 1 THEN 'reporting' ELSE '' END AS provisioning_failure_state`

const insertQueuedJobSQL = `INSERT INTO runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
		substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', (random() & 3) + 1, 1) ||
		substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))), ?, ?, ?, ?
	WHERE NOT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
		WHERE source_name = ? AND source_reference = ?)
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)
	ON CONFLICT(source_name, source_reference) DO NOTHING`

const queuedJobBySourceSQL = `SELECT ` + queuedJobColumns + ` FROM runnerfoundry_queued_jobs
	WHERE source_name = ? AND source_reference = ?`
const classifyQueueSourceSQL = `SELECT CASE
	WHEN EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones WHERE source_name = ? AND source_reference = ?)
		THEN 'source_terminal'
	WHEN EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs WHERE source_name = ? AND source_reference = ?)
		THEN 'processing'
	ELSE 'already_queued' END`
const oldestQueuedJobSQL = `SELECT ` + queuedJobColumns + ` FROM runnerfoundry_queued_jobs
	WHERE runner_pool = ? ORDER BY enqueued_at LIMIT 1`
const insertClaimedProcessingJobSQL = `INSERT INTO runnerfoundry_processing_jobs
	(job_id, source_name, source_reference, runner_pool, member_id, runner_name, claimed_at, inventory_endpoint_id)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
const insertTombstoneSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	VALUES (?, ?) ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs WHERE job_id = ?`
const cancelQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs
	WHERE source_name = ? AND source_reference = ?`
const markProcessingAcceptedSQL = `UPDATE runnerfoundry_processing_jobs SET runner_started = 1
	WHERE source_name = ? AND source_reference = ? AND runner_started = 0 AND provisioning_failure_reserved = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)`
const markProcessingAttemptAcceptedSQL = `UPDATE runnerfoundry_processing_jobs SET runner_started = 1
	WHERE source_name = ? AND source_reference = ? AND runner_name = ? AND runner_started = 0
	AND provisioning_failure_reserved = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)`
const sourceAcceptedProcessingSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
	WHERE source_name = ? AND source_reference = ?
	AND (runner_started = 1 OR provisioning_failure_reserved = 1))`
const attemptAcceptedProcessingSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
	WHERE source_name = ? AND source_reference = ? AND runner_name = ?
	AND (runner_started = 1 OR provisioning_failure_reserved = 1))`
const tombstoneDelayedAcceptanceSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT ?, ? WHERE EXISTS (SELECT 1 FROM runnerfoundry_queued_jobs
		WHERE source_name = ? AND source_reference = ?)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const processingByMemberSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE member_id = ? AND runner_pool = ?`
const processingByPoolSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE runner_pool = ?`
