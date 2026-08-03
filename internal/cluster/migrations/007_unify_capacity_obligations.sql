-- RunnerFoundry nodes are stopped while migrations run. These additions take a
-- brief ACCESS EXCLUSIVE lock. The bounded backfill changes only new ownership
-- and retry columns, so demand_id, observation_id, and queue_time remain exact.
ALTER TABLE scaler_demand
    ALTER COLUMN request_id DROP NOT NULL,
    ADD COLUMN app_id bigint,
    ADD COLUMN source text NOT NULL DEFAULT 'job_available'
        CHECK (source IN ('job_available', 'assigned_count')),
    ADD COLUMN assigned_observation_id bigint
        REFERENCES scaler_assigned_job_observations(assigned_observation_id) ON DELETE CASCADE,
    ADD COLUMN controller_epoch bigint NOT NULL DEFAULT 0 CHECK (controller_epoch >= 0),
    ADD COLUMN attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    ADD COLUMN next_attempt_at timestamptz;

ALTER TABLE scaler_demand
    DROP CONSTRAINT scaler_demand_scale_set_repository_request_id_key;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM scaler_demand demand
        WHERE (
            SELECT count(*)
            FROM scaler_registrations registration
            WHERE registration.repository_url = demand.repository
              AND registration.scale_set_name = demand.scale_set
        ) <> 1
    ) THEN
        RAISE EXCEPTION 'cannot migrate demand without one unambiguous App registration';
    END IF;
END;
$$;

UPDATE scaler_demand demand
SET app_id = registration.app_id,
    next_attempt_at = demand.queue_time
FROM scaler_registrations registration
WHERE registration.repository_url = demand.repository
  AND registration.scale_set_name = demand.scale_set;

ALTER TABLE scaler_demand
    ALTER COLUMN app_id SET NOT NULL,
    ALTER COLUMN next_attempt_at SET NOT NULL,
    ADD CONSTRAINT scaler_demand_registration FOREIGN KEY (app_id, repository, scale_set)
        REFERENCES scaler_registrations(app_id, repository_url, scale_set_name) ON DELETE CASCADE,
    ADD CONSTRAINT scaler_capacity_obligation_source CHECK (
        (source = 'job_available'
            AND request_id IS NOT NULL
            AND assigned_observation_id IS NULL
            AND controller_epoch = 0)
        OR
        (source = 'assigned_count'
            AND request_id IS NULL
            AND assigned_observation_id IS NOT NULL
            AND controller_epoch > 0)
    );

CREATE UNIQUE INDEX scaler_job_available_identity
    ON scaler_demand (app_id, scale_set, repository, request_id)
    WHERE source = 'job_available';

CREATE INDEX scaler_runnable_capacity_obligations
    ON scaler_demand (scale_set, next_attempt_at, queue_time, observation_id);

ALTER TABLE scaler_assignments
    DROP CONSTRAINT scaler_assignment_source,
    ADD CONSTRAINT scaler_assignment_source CHECK (
        demand_id IS NOT NULL OR assigned_observation_id IS NOT NULL
    );

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 7
WHERE singleton;

---- create above / drop below ----

-- Completed aggregate obligations are terminal history and have no schema-6
-- representation. Removing their demand row also removes their assignments.
DELETE FROM scaler_demand demand
WHERE demand.source = 'assigned_count'
  AND EXISTS (
      SELECT 1
      FROM scaler_assignments assignment
      WHERE assignment.demand_id = demand.demand_id
        AND assignment.state = 'complete'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM scaler_assignments assignment
      WHERE assignment.demand_id = demand.demand_id
        AND assignment.state <> 'complete'
  );

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM scaler_demand demand
        WHERE demand.source = 'assigned_count'
          AND NOT EXISTS (
              SELECT 1
              FROM scaler_assignments assignment
              WHERE assignment.demand_id = demand.demand_id
                AND assignment.state <> 'complete'
          )
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while unassigned aggregate capacity obligations remain';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM scaler_demand demand
        JOIN scaler_assignments assignment ON assignment.demand_id = demand.demand_id
        WHERE demand.source = 'assigned_count'
          AND assignment.state <> 'complete'
          AND assignment.assigned_observation_id IS NULL
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while unrepresentable aggregate assignments remain';
    END IF;
END;
$$;

UPDATE scaler_assignments assignment
SET demand_id = NULL
FROM scaler_demand demand
WHERE assignment.demand_id = demand.demand_id
  AND demand.source = 'assigned_count'
  AND assignment.state <> 'complete';

DELETE FROM scaler_demand WHERE source = 'assigned_count';

DROP INDEX scaler_runnable_capacity_obligations;
DROP INDEX scaler_job_available_identity;

ALTER TABLE scaler_assignments
    DROP CONSTRAINT scaler_assignment_source,
    ADD CONSTRAINT scaler_assignment_source CHECK (
        (demand_id IS NOT NULL) <> (assigned_observation_id IS NOT NULL)
    );

ALTER TABLE scaler_demand
    DROP CONSTRAINT scaler_capacity_obligation_source,
    DROP CONSTRAINT scaler_demand_registration,
    DROP COLUMN next_attempt_at,
    DROP COLUMN attempt,
    DROP COLUMN controller_epoch,
    DROP COLUMN assigned_observation_id,
    DROP COLUMN source,
    DROP COLUMN app_id,
    ALTER COLUMN request_id SET NOT NULL,
    ADD CONSTRAINT scaler_demand_scale_set_repository_request_id_key
        UNIQUE (scale_set, repository, request_id);

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 5
WHERE singleton;
