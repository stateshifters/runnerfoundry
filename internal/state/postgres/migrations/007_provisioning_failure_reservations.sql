SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.runnerfoundry_processing_jobs
    ADD COLUMN provisioning_failure_reserved boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT runnerfoundry_processing_jobs_provisioning_failure_fence
        CHECK (NOT (runner_started AND provisioning_failure_reserved)) NOT VALID;

CREATE TABLE public.runnerfoundry_provisioning_failures (
    job_id uuid NOT NULL,
    runner_name text NOT NULL,
    is_reported boolean NOT NULL DEFAULT false,
    CONSTRAINT runnerfoundry_provisioning_failures_pkey PRIMARY KEY (job_id),
    CONSTRAINT runnerfoundry_provisioning_failures_job_id_fkey
        FOREIGN KEY (job_id)
        REFERENCES public.runnerfoundry_processing_jobs (job_id)
        ON DELETE RESTRICT,
    CONSTRAINT runnerfoundry_provisioning_failures_runner_name_nonempty
        CHECK (btrim(runner_name) <> '')
);

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TABLE public.runnerfoundry_provisioning_failures;

ALTER TABLE public.runnerfoundry_processing_jobs
    DROP CONSTRAINT runnerfoundry_processing_jobs_provisioning_failure_fence,
    DROP COLUMN provisioning_failure_reserved;
