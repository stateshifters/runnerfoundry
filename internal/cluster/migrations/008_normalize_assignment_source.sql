-- Migration 008 is a stopped-cluster contraction. The preflight performs
-- aggregate-only scans before the first mutation and reports no row payloads.
DO $$
DECLARE
    missing_demand_count bigint;
    assignment_source_count bigint;
    assignment_identity_count bigint;
    lifecycle_identity_count bigint;
    lifecycle_phase_timestamp_count bigint;
BEGIN
    SELECT
        (SELECT count(*)
         FROM scaler_assignments assignment
         LEFT JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
         WHERE demand.demand_id IS NULL),
        (SELECT count(*)
         FROM scaler_assignments assignment
         JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
         WHERE assignment.assigned_observation_id IS DISTINCT FROM demand.assigned_observation_id
            OR assignment.controller_epoch <> demand.controller_epoch),
        (SELECT count(*)
         FROM scaler_assignments assignment
         JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
         WHERE (demand.source = 'assigned_count'
                    AND assignment.state NOT IN ('ready', 'starting', 'running', 'complete'))
            OR (demand.source = 'job_available'
                    AND assignment.state NOT IN ('reserved', 'acquiring', 'ready', 'starting', 'running', 'complete'))
            OR ((assignment.installation_id IS NULL) <> (assignment.github_scale_set_id IS NULL))),
        (SELECT count(*)
         FROM scaler_runner_lifecycle lifecycle
         JOIN scaler_assignments assignment ON assignment.assignment_id = lifecycle.assignment_id
         JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
         WHERE lifecycle.node_id <> assignment.node_id
            OR lifecycle.scale_set <> demand.scale_set
            OR lifecycle.repository <> demand.repository
            OR (demand.source = 'job_available' AND lifecycle.request_id <> demand.request_id)
            OR (demand.source = 'assigned_count'
                AND NOT (lifecycle.request_id = 0
                    OR (lifecycle.request_id > 0 AND lifecycle.phase IN ('busy', 'reap_pending'))))
            OR lifecycle.installation_id <> COALESCE(assignment.installation_id, 0)
            OR lifecycle.github_scale_set_id <> COALESCE(assignment.github_scale_set_id, 0)),
        (SELECT count(*)
         FROM scaler_runner_lifecycle lifecycle
         WHERE lifecycle.updated_at < lifecycle.created_at
            OR lifecycle.github_runner_id < 0
            OR (lifecycle.phase = 'provisioning' AND lifecycle.waiting_since IS NOT NULL)
            OR (lifecycle.phase IN ('waiting_for_job', 'busy')
                AND (lifecycle.waiting_since IS NULL
                    OR lifecycle.waiting_since < lifecycle.created_at
                    OR lifecycle.waiting_since > lifecycle.updated_at))
            OR (lifecycle.phase = 'reap_pending' AND lifecycle.waiting_since IS NOT NULL
                AND (lifecycle.waiting_since < lifecycle.created_at
                    OR lifecycle.waiting_since > lifecycle.updated_at))
            OR ((lifecycle.phase = 'reap_pending') <> (btrim(lifecycle.reap_reason) <> '')))
    INTO missing_demand_count, assignment_source_count, assignment_identity_count,
         lifecycle_identity_count, lifecycle_phase_timestamp_count;

    IF missing_demand_count + assignment_source_count + assignment_identity_count
       + lifecycle_identity_count + lifecycle_phase_timestamp_count > 0 THEN
        RAISE EXCEPTION
            'cannot normalize assignment source: missing_demand=%, assignment_source=%, assignment_identity=%, lifecycle_identity=%, lifecycle_phase_timestamp=%',
            missing_demand_count, assignment_source_count, assignment_identity_count,
            lifecycle_identity_count, lifecycle_phase_timestamp_count;
    END IF;
END;
$$;

ALTER TABLE scaler_demand
    ADD CONSTRAINT scaler_demand_id_source_key UNIQUE (demand_id, source);

ALTER TABLE scaler_assignments ADD COLUMN source text;
UPDATE scaler_assignments assignment
SET source = demand.source
FROM scaler_demand demand
WHERE demand.demand_id = assignment.demand_id;

DROP INDEX scaler_active_observed_assignments;

ALTER TABLE scaler_assignments
    ALTER COLUMN demand_id SET NOT NULL,
    ALTER COLUMN source SET NOT NULL,
    DROP CONSTRAINT scaler_observed_assignment_state,
    DROP CONSTRAINT scaler_assignment_source,
    DROP COLUMN assigned_observation_id,
    DROP COLUMN controller_epoch,
    ADD CONSTRAINT scaler_assignment_demand_source
        FOREIGN KEY (demand_id, source)
        REFERENCES scaler_demand(demand_id, source) ON DELETE CASCADE,
    ADD CONSTRAINT scaler_assignment_source_state CHECK (
        (source = 'job_available'
            AND state IN ('reserved', 'acquiring', 'ready', 'starting', 'running', 'complete'))
        OR
        (source = 'assigned_count'
            AND state IN ('ready', 'starting', 'running', 'complete'))
    );

ALTER TABLE scaler_runner_lifecycle
    ADD CONSTRAINT scaler_runner_lifecycle_timestamps CHECK (
        updated_at >= created_at
        AND (waiting_since IS NULL OR waiting_since BETWEEN created_at AND updated_at)
    ),
    ADD CONSTRAINT scaler_runner_lifecycle_phase_timestamps CHECK (
        (phase = 'provisioning' AND waiting_since IS NULL)
        OR (phase IN ('waiting_for_job', 'busy') AND waiting_since IS NOT NULL)
        OR phase = 'reap_pending'
    ),
    ADD CONSTRAINT scaler_runner_lifecycle_reap_reason CHECK (
        (phase = 'reap_pending') = (btrim(reap_reason) <> '')
    ),
    ADD CONSTRAINT scaler_runner_lifecycle_identity CHECK (
        installation_id >= 0 AND github_scale_set_id >= 0 AND github_runner_id >= 0
    );

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 8
WHERE singleton;

---- create above / drop below ----

ALTER TABLE scaler_runner_lifecycle
    DROP CONSTRAINT scaler_runner_lifecycle_identity,
    DROP CONSTRAINT scaler_runner_lifecycle_reap_reason,
    DROP CONSTRAINT scaler_runner_lifecycle_phase_timestamps,
    DROP CONSTRAINT scaler_runner_lifecycle_timestamps;

ALTER TABLE scaler_assignments
    DROP CONSTRAINT scaler_assignment_source_state,
    DROP CONSTRAINT scaler_assignment_demand_source,
    ADD COLUMN assigned_observation_id bigint
        REFERENCES scaler_assigned_job_observations(assigned_observation_id) ON DELETE CASCADE,
    ADD COLUMN controller_epoch bigint NOT NULL DEFAULT 0 CHECK (controller_epoch >= 0);

UPDATE scaler_assignments assignment
SET assigned_observation_id = demand.assigned_observation_id,
    controller_epoch = demand.controller_epoch
FROM scaler_demand demand
WHERE demand.demand_id = assignment.demand_id;

CREATE INDEX scaler_active_observed_assignments
    ON scaler_assignments(assigned_observation_id) WHERE state <> 'complete';

ALTER TABLE scaler_assignments
    ADD CONSTRAINT scaler_assignment_source CHECK (
        demand_id IS NOT NULL OR assigned_observation_id IS NOT NULL
    ),
    ADD CONSTRAINT scaler_observed_assignment_state CHECK (
        assigned_observation_id IS NULL OR state IN ('ready', 'starting', 'running', 'complete')
    ),
    DROP COLUMN source,
    ALTER COLUMN demand_id DROP NOT NULL;

ALTER TABLE scaler_demand
    DROP CONSTRAINT scaler_demand_id_source_key;

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 7
WHERE singleton;
