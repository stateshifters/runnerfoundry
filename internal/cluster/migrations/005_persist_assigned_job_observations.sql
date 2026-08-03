CREATE TABLE scaler_assigned_job_observations (
    assigned_observation_id bigserial PRIMARY KEY,
    app_id bigint NOT NULL,
    repository_url text NOT NULL,
    scale_set text NOT NULL CHECK (btrim(scale_set) <> ''),
    assigned_jobs integer NOT NULL CHECK (assigned_jobs >= 0),
    controller_epoch bigint NOT NULL CHECK (controller_epoch > 0),
    observed_at timestamptz NOT NULL,
    UNIQUE (app_id, repository_url, scale_set),
    FOREIGN KEY (app_id, repository_url, scale_set)
        REFERENCES scaler_registrations(app_id, repository_url, scale_set_name) ON DELETE RESTRICT
);

ALTER TABLE scaler_assignments ALTER COLUMN demand_id DROP NOT NULL;
ALTER TABLE scaler_assignments
    ADD COLUMN assigned_observation_id bigint
        REFERENCES scaler_assigned_job_observations(assigned_observation_id) ON DELETE CASCADE,
    ADD COLUMN controller_epoch bigint NOT NULL DEFAULT 0 CHECK (controller_epoch >= 0),
    ADD CONSTRAINT scaler_assignment_source CHECK (
        (demand_id IS NOT NULL) <> (assigned_observation_id IS NOT NULL)
    ),
    ADD CONSTRAINT scaler_observed_assignment_state CHECK (
        assigned_observation_id IS NULL OR state IN ('ready', 'starting', 'running', 'complete')
    );

CREATE INDEX scaler_active_observed_assignments
    ON scaler_assignments(assigned_observation_id) WHERE state <> 'complete';

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 5
WHERE singleton;

---- create above / drop below ----

UPDATE scaler_runner_lifecycle lifecycle
SET phase = 'reap_pending', reap_reason = 'schema_rollback', updated_at = clock_timestamp()
WHERE lifecycle.assignment_id IN (
    SELECT assignment_id FROM scaler_assignments WHERE assigned_observation_id IS NOT NULL
);

UPDATE scaler_libvirt_runner_lifecycle legacy
SET reap_requested = true
WHERE EXISTS (
    SELECT 1 FROM scaler_runner_lifecycle lifecycle
    WHERE lifecycle.node_id = legacy.node_id
      AND lifecycle.scale_set = legacy.scale_set
      AND lifecycle.repository = legacy.repository
      AND lifecycle.runner_name = legacy.runner_name
      AND lifecycle.phase = 'reap_pending'
      AND lifecycle.reap_reason = 'schema_rollback'
);

DELETE FROM scaler_assignments WHERE assigned_observation_id IS NOT NULL;
DROP INDEX scaler_active_observed_assignments;
ALTER TABLE scaler_assignments
    DROP CONSTRAINT scaler_observed_assignment_state,
    DROP CONSTRAINT scaler_assignment_source,
    DROP COLUMN controller_epoch,
    DROP COLUMN assigned_observation_id;
ALTER TABLE scaler_assignments ALTER COLUMN demand_id SET NOT NULL;
DROP TABLE scaler_assigned_job_observations;

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 1
WHERE singleton;
