-- Migration 009 is a stopped-cluster contraction. The aggregate preflight
-- completes before the first mutation and never reports lifecycle payloads.
DO $$
DECLARE
    legacy_only_count bigint;
    conflicting_count bigint;
BEGIN
    SELECT
        (SELECT count(*)
         FROM scaler_libvirt_runner_lifecycle legacy
         LEFT JOIN scaler_runner_lifecycle lifecycle
           ON lifecycle.node_id = legacy.node_id
          AND lifecycle.scale_set = legacy.scale_set
          AND lifecycle.repository = legacy.repository
          AND lifecycle.runner_name = legacy.runner_name
         WHERE lifecycle.node_id IS NULL),
        (SELECT count(*)
         FROM scaler_libvirt_runner_lifecycle legacy
         JOIN scaler_runner_lifecycle lifecycle
           ON lifecycle.node_id = legacy.node_id
          AND lifecycle.scale_set = legacy.scale_set
          AND lifecycle.repository = legacy.repository
          AND lifecycle.runner_name = legacy.runner_name
         WHERE lifecycle.provider <> 'libvirt'
            OR lifecycle.request_id <> legacy.request_id
            OR (lifecycle.phase = 'reap_pending') <> legacy.reap_requested)
    INTO legacy_only_count, conflicting_count;

    IF legacy_only_count + conflicting_count > 0 THEN
        RAISE EXCEPTION
            'cannot retire legacy libvirt lifecycle: legacy_only=%, conflicting=%',
            legacy_only_count, conflicting_count;
    END IF;
END;
$$;

-- The stopped-cluster rollout accepts the brief ACCESS EXCLUSIVE locks needed
-- to remove the expired trigger and legacy table after the preflight succeeds.
DROP TRIGGER scaler_mirror_legacy_libvirt_lifecycle_write
    ON scaler_libvirt_runner_lifecycle;
DROP FUNCTION scaler_mirror_legacy_libvirt_lifecycle();
DROP TABLE scaler_libvirt_runner_lifecycle;

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 9
WHERE singleton;

---- create above / drop below ----

UPDATE scaler_schema_compatibility
SET oldest_binary_schema_generation = 8
WHERE singleton;

CREATE TABLE scaler_libvirt_runner_lifecycle (
    node_id text NOT NULL REFERENCES scaler_nodes(node_id),
    scale_set text NOT NULL CHECK (btrim(scale_set) <> ''),
    repository text NOT NULL CHECK (btrim(repository) <> ''),
    request_id bigint NOT NULL CHECK (request_id >= 0),
    runner_name text NOT NULL CHECK (btrim(runner_name) <> ''),
    reap_requested boolean NOT NULL DEFAULT false,
    PRIMARY KEY (node_id, scale_set, repository, runner_name)
);
CREATE INDEX scaler_pending_libvirt_reaps_by_node
    ON scaler_libvirt_runner_lifecycle(node_id) WHERE reap_requested;
CREATE INDEX scaler_pending_libvirt_reaps_by_demand
    ON scaler_libvirt_runner_lifecycle(scale_set, repository, request_id) WHERE reap_requested;

-- Reconstruct the complete legacy representation before restoring its trigger.
-- This preserves generic lifecycle timestamps and history byte-for-byte.
INSERT INTO scaler_libvirt_runner_lifecycle (
    node_id, scale_set, repository, request_id, runner_name, reap_requested
)
SELECT node_id, scale_set, repository, request_id, runner_name,
       phase = 'reap_pending'
FROM scaler_runner_lifecycle
WHERE provider = 'libvirt';

CREATE FUNCTION scaler_mirror_legacy_libvirt_lifecycle() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM scaler_runner_lifecycle
        WHERE node_id = OLD.node_id AND scale_set = OLD.scale_set
          AND repository = OLD.repository AND runner_name = OLD.runner_name;
        RETURN OLD;
    END IF;
    INSERT INTO scaler_runner_lifecycle (
        node_id, scale_set, repository, provider, runner_name, request_id,
        phase, created_at, waiting_since, updated_at, reap_reason
    ) VALUES (
        NEW.node_id, NEW.scale_set, NEW.repository, 'libvirt', NEW.runner_name, NEW.request_id,
        CASE WHEN NEW.reap_requested THEN 'reap_pending' ELSE 'waiting_for_job' END,
        clock_timestamp(), CASE WHEN NEW.reap_requested THEN NULL ELSE clock_timestamp() END, clock_timestamp(),
        CASE WHEN NEW.reap_requested THEN 'legacy_reap_requested' ELSE '' END
    )
    ON CONFLICT (node_id, scale_set, repository, runner_name) DO UPDATE SET
        request_id = EXCLUDED.request_id,
        phase = CASE WHEN NEW.reap_requested THEN 'reap_pending' ELSE scaler_runner_lifecycle.phase END,
        updated_at = CASE
            WHEN scaler_runner_lifecycle.reap_reason = '' THEN clock_timestamp()
            ELSE scaler_runner_lifecycle.updated_at
        END,
        reap_reason = CASE
            WHEN NEW.reap_requested AND scaler_runner_lifecycle.reap_reason = '' THEN 'legacy_reap_requested'
            ELSE scaler_runner_lifecycle.reap_reason
        END;
    RETURN NEW;
END;
$$;

CREATE TRIGGER scaler_mirror_legacy_libvirt_lifecycle_write
AFTER INSERT OR UPDATE OR DELETE ON scaler_libvirt_runner_lifecycle
FOR EACH ROW EXECUTE FUNCTION scaler_mirror_legacy_libvirt_lifecycle();
