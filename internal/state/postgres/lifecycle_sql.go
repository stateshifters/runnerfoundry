package postgres

const processingByIDSQL = `SELECT ` + processingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE job_id = $1`
const processingByClaimIDSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE job_id = $1`
const processingByPoolSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE runner_pool = $1`
const recoverProcessingSQL = `INSERT INTO public.runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT job_id, source_name, source_reference, runner_pool, $4
	FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND processing.claimed_at = $2
	AND processing.inventory_endpoint_id = $3 AND NOT processing.runner_started
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	ON CONFLICT(source_name, source_reference) DO NOTHING
	RETURNING ` + queuedJobColumns
const deleteRecoveredProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND processing.claimed_at = $2
	AND processing.inventory_endpoint_id = $3 AND NOT processing.runner_started
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)`
const retryProcessingSQL = `INSERT INTO public.runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool)
	SELECT job_id, source_name, source_reference, runner_pool
	FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND NOT processing.runner_started
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	RETURNING ` + queuedJobColumns
const deleteRetriedProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND NOT runner_started`
const deleteTerminalProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND EXISTS (
		SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)`
const processingExistsSQL = `SELECT EXISTS (SELECT 1 FROM public.runnerfoundry_processing_jobs WHERE job_id = $1)`
