SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30min';

ALTER TABLE public.runnerfoundry_processing_jobs
    VALIDATE CONSTRAINT runnerfoundry_processing_jobs_provisioning_failure_fence;

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.runnerfoundry_processing_jobs
    DROP CONSTRAINT runnerfoundry_processing_jobs_provisioning_failure_fence,
    ADD CONSTRAINT runnerfoundry_processing_jobs_provisioning_failure_fence
        CHECK (NOT (runner_started AND provisioning_failure_reserved)) NOT VALID;
