SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DELETE FROM public.runnerfoundry_processing_jobs;

ALTER TABLE public.runnerfoundry_processing_jobs
    ADD COLUMN runner_name text NOT NULL,
    ADD CONSTRAINT runnerfoundry_processing_jobs_runner_name_nonempty
        CHECK (btrim(runner_name) <> '');

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.runnerfoundry_processing_jobs
    DROP CONSTRAINT runnerfoundry_processing_jobs_runner_name_nonempty,
    DROP COLUMN runner_name;
