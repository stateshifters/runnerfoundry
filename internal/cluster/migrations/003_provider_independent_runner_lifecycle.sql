CREATE TABLE scaler_runner_lifecycle (
    node_id text NOT NULL REFERENCES scaler_nodes(node_id),
    scale_set text NOT NULL CHECK (btrim(scale_set) <> ''),
    repository text NOT NULL CHECK (btrim(repository) <> ''),
    provider text NOT NULL CHECK (provider IN ('docker', 'libvirt')),
    runner_name text NOT NULL CHECK (btrim(runner_name) <> ''),
    request_id bigint NOT NULL CHECK (request_id >= 0),
    assignment_id bigint REFERENCES scaler_assignments(assignment_id) ON DELETE SET NULL,
    installation_id bigint NOT NULL DEFAULT 0,
    github_scale_set_id integer NOT NULL DEFAULT 0,
    github_runner_id bigint NOT NULL DEFAULT 0 CHECK (github_runner_id >= 0),
    phase text NOT NULL CHECK (phase IN ('provisioning', 'waiting_for_job', 'busy', 'reap_pending')),
    created_at timestamptz NOT NULL,
    waiting_since timestamptz,
    updated_at timestamptz NOT NULL,
    reap_reason text NOT NULL DEFAULT '',
    PRIMARY KEY (node_id, scale_set, repository, runner_name)
);
CREATE INDEX scaler_pending_runner_reaps_by_node
    ON scaler_runner_lifecycle(node_id) WHERE phase = 'reap_pending';
CREATE INDEX scaler_waiting_runners_by_scale_set
    ON scaler_runner_lifecycle(scale_set, repository, waiting_since)
    WHERE phase = 'waiting_for_job';
CREATE INDEX scaler_runner_lifecycle_by_demand
    ON scaler_runner_lifecycle(scale_set, repository, request_id);

INSERT INTO scaler_runner_lifecycle (
    node_id, scale_set, repository, provider, runner_name, request_id,
    phase, created_at, waiting_since, updated_at, reap_reason
)
SELECT node_id, scale_set, repository, 'libvirt', runner_name, request_id,
       CASE WHEN reap_requested THEN 'reap_pending' ELSE 'waiting_for_job' END,
       clock_timestamp(),
       CASE WHEN reap_requested THEN NULL ELSE clock_timestamp() END,
       clock_timestamp(),
       CASE WHEN reap_requested THEN 'legacy_reap_requested' ELSE '' END
FROM scaler_libvirt_runner_lifecycle;

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

---- create above / drop below ----

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM scaler_runner_lifecycle
        WHERE provider = 'docker' AND phase = 'reap_pending'
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while Docker runner cleanup obligations remain';
    END IF;
END;
$$;

DROP TRIGGER scaler_mirror_legacy_libvirt_lifecycle_write ON scaler_libvirt_runner_lifecycle;
DROP FUNCTION scaler_mirror_legacy_libvirt_lifecycle();
DROP TABLE scaler_runner_lifecycle;
