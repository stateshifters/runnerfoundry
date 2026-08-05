ALTER TABLE assigned_capacity_observations
    ADD COLUMN running_jobs INTEGER NOT NULL DEFAULT 0 CHECK(running_jobs >= 0);
ALTER TABLE assigned_capacity_observations
    ADD COLUMN registered_runners INTEGER NOT NULL DEFAULT 0 CHECK(registered_runners >= 0);
ALTER TABLE assigned_capacity_observations
    ADD COLUMN busy_runners INTEGER NOT NULL DEFAULT 0 CHECK(busy_runners >= 0);

ALTER TABLE demand
    ADD COLUMN blocked_reason TEXT CHECK(
        blocked_reason IS NULL
        OR (source = 'assigned_count' AND blocked_reason = 'stranded_upstream_assignment')
    );

DROP INDEX demand_runnable_fifo;
CREATE INDEX demand_runnable_fifo
    ON demand(scale_set, next_attempt_at, queue_time, observation_order)
    WHERE blocked_reason IS NULL;

PRAGMA user_version = 9;
