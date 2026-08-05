-- RunnerFoundry nodes are stopped while this migration runs. Constant defaults
-- preserve existing observations without a data backfill.
ALTER TABLE scaler_assigned_job_observations
    ADD COLUMN running_jobs integer NOT NULL DEFAULT 0 CHECK (running_jobs >= 0),
    ADD COLUMN registered_runners integer NOT NULL DEFAULT 0 CHECK (registered_runners >= 0),
    ADD COLUMN busy_runners integer NOT NULL DEFAULT 0 CHECK (busy_runners >= 0);

ALTER TABLE scaler_demand
    ADD COLUMN blocked_reason text,
    ADD CONSTRAINT scaler_demand_blocked_reason CHECK (
        blocked_reason IS NULL
        OR (source = 'assigned_count' AND blocked_reason = 'stranded_upstream_assignment')
    );

DROP INDEX scaler_runnable_capacity_obligations;
CREATE INDEX scaler_runnable_capacity_obligations
    ON scaler_demand (scale_set, next_attempt_at, queue_time, observation_id)
    WHERE blocked_reason IS NULL;

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 10
WHERE singleton;

---- create above / drop below ----

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM scaler_demand WHERE blocked_reason IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot downgrade while stranded aggregate capacity obligations remain blocked';
    END IF;
END;
$$;

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 9
WHERE singleton;

DROP INDEX scaler_runnable_capacity_obligations;
CREATE INDEX scaler_runnable_capacity_obligations
    ON scaler_demand (scale_set, next_attempt_at, queue_time, observation_id);

ALTER TABLE scaler_demand
    DROP CONSTRAINT scaler_demand_blocked_reason,
    DROP COLUMN blocked_reason;

ALTER TABLE scaler_assigned_job_observations
    DROP COLUMN busy_runners,
    DROP COLUMN registered_runners,
    DROP COLUMN running_jobs;
