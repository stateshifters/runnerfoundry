SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.runnerfoundry_notify_queued_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('runnerfoundry_queued_jobs', NEW.runner_pool);
    RETURN NEW;
END;
$$;

CREATE TRIGGER runnerfoundry_queued_jobs_notify
AFTER INSERT ON public.runnerfoundry_queued_jobs
FOR EACH ROW
EXECUTE FUNCTION public.runnerfoundry_notify_queued_job();

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TRIGGER runnerfoundry_queued_jobs_notify ON public.runnerfoundry_queued_jobs;
DROP FUNCTION public.runnerfoundry_notify_queued_job();
