package postgres

const sourceByJobIDSQL = `SELECT source_name, source_reference
	FROM public.runnerfoundry_queued_jobs WHERE job_id = $1
	UNION ALL
	SELECT source_name, source_reference
	FROM public.runnerfoundry_processing_jobs WHERE job_id = $1`
const insertTombstoneSQL = `INSERT INTO public.runnerfoundry_source_tombstones(source_name, source_reference)
	VALUES($1, $2) ON CONFLICT(source_name, source_reference) DO NOTHING`
const processingAttemptExistsSQL = `SELECT EXISTS (SELECT 1 FROM public.runnerfoundry_processing_jobs
	WHERE source_name = $1 AND source_reference = $2 AND runner_name = $3)`
const sourceTombstonedSQL = `SELECT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones
	WHERE source_name = $1 AND source_reference = $2)`
const markProcessingAcceptedSQL = `UPDATE public.runnerfoundry_processing_jobs SET runner_started = true
	WHERE source_name = $1 AND source_reference = $2 AND NOT runner_started AND NOT provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones
		WHERE source_name = $1 AND source_reference = $2)`
const markProcessingAttemptAcceptedSQL = `UPDATE public.runnerfoundry_processing_jobs SET runner_started = true
	WHERE source_name = $1 AND source_reference = $2 AND runner_name = $3 AND NOT runner_started
	AND NOT provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones
		WHERE source_name = $1 AND source_reference = $2)`
const tombstoneDelayedAcceptanceSQL = `INSERT INTO public.runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM public.runnerfoundry_queued_jobs
		WHERE source_name = $1 AND source_reference = $2)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const runnerAcceptanceAlreadyAppliedSQL = `SELECT
	EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones WHERE source_name = $1 AND source_reference = $2)
	OR EXISTS (SELECT 1 FROM public.runnerfoundry_processing_jobs
		WHERE source_name = $1 AND source_reference = $2
		AND (runner_started OR provisioning_failure_reserved))`
const attemptAcceptanceAlreadyAppliedSQL = `SELECT
	EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones WHERE source_name = $1 AND source_reference = $2)
	OR EXISTS (SELECT 1 FROM public.runnerfoundry_processing_jobs
		WHERE source_name = $1 AND source_reference = $2 AND runner_name = $3
		AND (runner_started OR provisioning_failure_reserved))`

const processingByClaimIDSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE job_id = $1`
const processingByPoolSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE runner_pool = $1`
const recoverProcessingSQL = `INSERT INTO public.runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT job_id, source_name, source_reference, runner_pool, $4
	FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND processing.claimed_at = $2
	AND processing.inventory_endpoint_id = $3 AND processing.runner_name = $5 AND NOT processing.runner_started
	AND NOT processing.provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	ON CONFLICT(source_name, source_reference) DO NOTHING
	RETURNING ` + queuedJobColumns
const deleteRecoveredProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND processing.claimed_at = $2
	AND processing.inventory_endpoint_id = $3 AND processing.runner_name = $4 AND NOT processing.runner_started
	AND NOT processing.provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)`
const retryProcessingSQL = `INSERT INTO public.runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool)
	SELECT job_id, source_name, source_reference, runner_pool
	FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND processing.claimed_at = $2 AND processing.inventory_endpoint_id = $3
	AND processing.runner_name = $4 AND NOT processing.runner_started AND NOT processing.provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	RETURNING ` + queuedJobColumns
const deleteRetriedProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND claimed_at = $2 AND inventory_endpoint_id = $3 AND runner_name = $4
	AND NOT runner_started AND NOT provisioning_failure_reserved`
const deleteTerminalProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND EXISTS (
		SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)
	AND NOT provisioning_failure_reserved`
const processingExistsSQL = `SELECT EXISTS (SELECT 1 FROM public.runnerfoundry_processing_jobs WHERE job_id = $1)`
