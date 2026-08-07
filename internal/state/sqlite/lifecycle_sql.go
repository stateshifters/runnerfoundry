package sqlite

const processingByIDSQL = `SELECT ` + processingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE job_id = ?`
const processingByClaimIDSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE job_id = ?`
const recoverProcessingSQL = `INSERT INTO runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT job_id, source_name, source_reference, runner_pool, ?
	FROM runnerfoundry_processing_jobs processing
	WHERE processing.job_id = ? AND processing.claimed_at = ?
	AND processing.inventory_endpoint_id = ? AND processing.runner_started = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteRecoveredProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND claimed_at = ? AND inventory_endpoint_id = ? AND runner_started = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)`
const retryProcessingSQL = `INSERT INTO runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT job_id, source_name, source_reference, runner_pool,
		CAST((julianday('now') - 2440587.5) * 86400000000000 AS INTEGER)
	FROM runnerfoundry_processing_jobs processing
	WHERE processing.job_id = ? AND processing.runner_started = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)`
const deleteRetriedProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND runner_started = 0`
const deleteTerminalProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND EXISTS (
		SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)`
const processingExistsSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs WHERE job_id = ?)`
