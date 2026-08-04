DROP TRIGGER mirror_libvirt_runner_lifecycle_insert;
DROP TRIGGER mirror_libvirt_runner_lifecycle_update;
DROP TRIGGER mirror_libvirt_runner_lifecycle_delete;

ALTER TABLE runner_lifecycle RENAME TO runner_lifecycle_v6;

CREATE TABLE runner_lifecycle (
    node_id TEXT NOT NULL DEFAULT '',
    scale_set TEXT NOT NULL CHECK(trim(scale_set) <> ''),
    repository TEXT NOT NULL CHECK(trim(repository) <> ''),
    provider TEXT NOT NULL CHECK(provider IN ('docker', 'libvirt')),
    runner_name TEXT NOT NULL CHECK(trim(runner_name) <> ''),
    request_id INTEGER NOT NULL CHECK(request_id >= 0),
    assignment_id INTEGER REFERENCES demand(demand_id) ON DELETE SET NULL,
    installation_id INTEGER NOT NULL DEFAULT 0 CHECK(installation_id >= 0),
    github_scale_set_id INTEGER NOT NULL DEFAULT 0 CHECK(github_scale_set_id >= 0),
    github_runner_id INTEGER NOT NULL DEFAULT 0 CHECK(github_runner_id >= 0),
    phase TEXT NOT NULL CHECK(phase IN ('provisioning', 'waiting_for_job', 'busy', 'reap_pending')),
    created_at INTEGER NOT NULL CHECK(created_at > 0),
    waiting_since INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    reap_reason TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(scale_set, repository, runner_name),
    CHECK(updated_at >= created_at),
    CHECK((phase = 'provisioning' AND waiting_since = 0)
       OR (phase IN ('waiting_for_job', 'busy') AND waiting_since BETWEEN created_at AND updated_at)
       OR (phase = 'reap_pending' AND (waiting_since = 0 OR waiting_since BETWEEN created_at AND updated_at))),
    CHECK((phase = 'reap_pending') = (reap_reason <> ''))
);

INSERT INTO runner_lifecycle(
    node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
    installation_id, github_scale_set_id, github_runner_id, phase, created_at,
    waiting_since, updated_at, reap_reason
)
SELECT node_id, scale_set, repository, provider, runner_name, request_id,
       CASE WHEN assignment_id > 0 THEN assignment_id END,
       installation_id, github_scale_set_id, github_runner_id, phase, created_at,
       waiting_since, updated_at, reap_reason
FROM runner_lifecycle_v6;

DROP TABLE runner_lifecycle_v6;

CREATE INDEX runner_lifecycle_pending_reaps
    ON runner_lifecycle(scale_set, repository, phase);

CREATE TRIGGER runner_lifecycle_assignment_insert
BEFORE INSERT ON runner_lifecycle BEGIN
    SELECT RAISE(ABORT, 'runner lifecycle assignment source mismatch')
    WHERE NEW.assignment_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM demand
        WHERE demand_id = NEW.assignment_id
          AND scale_set = NEW.scale_set
          AND repository = NEW.repository
          AND ((source = 'job_available' AND request_id = NEW.request_id)
            OR (source = 'assigned_count' AND
                (NEW.request_id = 0 OR
                 (NEW.request_id > 0 AND NEW.phase IN ('busy', 'reap_pending')))))
    );
END;

CREATE TRIGGER runner_lifecycle_assignment_update
BEFORE UPDATE OF scale_set, repository, request_id, assignment_id ON runner_lifecycle BEGIN
    SELECT RAISE(ABORT, 'runner lifecycle assignment source mismatch')
    WHERE NEW.assignment_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM demand
        WHERE demand_id = NEW.assignment_id
          AND scale_set = NEW.scale_set
          AND repository = NEW.repository
          AND ((source = 'job_available' AND request_id = NEW.request_id)
            OR (source = 'assigned_count' AND
                (NEW.request_id = 0 OR
                 (NEW.request_id > 0 AND NEW.phase IN ('busy', 'reap_pending')))))
    );
END;

CREATE TRIGGER demand_assignment_source_update
BEFORE UPDATE OF scale_set, repository, request_id, source ON demand BEGIN
    SELECT RAISE(ABORT, 'demand has assigned lifecycle state')
    WHERE EXISTS (
        SELECT 1 FROM runner_lifecycle
        WHERE assignment_id = OLD.demand_id
          AND (scale_set <> NEW.scale_set
            OR repository <> NEW.repository
            OR (NEW.source = 'job_available' AND request_id <> NEW.request_id))
    );
END;

CREATE TRIGGER mirror_libvirt_runner_lifecycle_insert
AFTER INSERT ON libvirt_runner_lifecycle BEGIN
    INSERT INTO runner_lifecycle(
        scale_set, repository, provider, runner_name, request_id, assignment_id,
        phase, created_at, waiting_since, updated_at, reap_reason
    ) VALUES (
        NEW.scale_set, NEW.repository, 'libvirt', NEW.runner_name, NEW.request_id, NULL,
        CASE WHEN NEW.reap_requested = 1 THEN 'reap_pending' ELSE 'waiting_for_job' END,
        unixepoch('now') * 1000000000,
        CASE WHEN NEW.reap_requested = 1 THEN 0 ELSE unixepoch('now') * 1000000000 END,
        unixepoch('now') * 1000000000,
        CASE WHEN NEW.reap_requested = 1 THEN 'legacy_reap_requested' ELSE '' END
    )
    ON CONFLICT(scale_set, repository, runner_name) DO UPDATE SET
        request_id = excluded.request_id,
        phase = CASE WHEN NEW.reap_requested = 1 THEN 'reap_pending' ELSE runner_lifecycle.phase END,
        reap_reason = CASE
            WHEN NEW.reap_requested = 1 AND runner_lifecycle.reap_reason = '' THEN 'legacy_reap_requested'
            ELSE runner_lifecycle.reap_reason END;
END;

CREATE TRIGGER mirror_libvirt_runner_lifecycle_update
AFTER UPDATE ON libvirt_runner_lifecycle BEGIN
    UPDATE runner_lifecycle
    SET phase = CASE WHEN NEW.reap_requested = 1 THEN 'reap_pending' ELSE phase END,
        reap_reason = CASE
            WHEN NEW.reap_requested = 1 AND reap_reason = '' THEN 'legacy_reap_requested'
            ELSE reap_reason END
    WHERE scale_set = NEW.scale_set AND repository = NEW.repository
      AND runner_name = NEW.runner_name AND request_id = NEW.request_id;
END;

CREATE TRIGGER mirror_libvirt_runner_lifecycle_delete
AFTER DELETE ON libvirt_runner_lifecycle BEGIN
    DELETE FROM runner_lifecycle
    WHERE scale_set = OLD.scale_set AND repository = OLD.repository
      AND runner_name = OLD.runner_name;
END;

PRAGMA user_version = 7;
