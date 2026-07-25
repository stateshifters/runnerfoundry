CREATE TABLE scaler_libvirt_runner_lifecycle (
    node_id text NOT NULL REFERENCES scaler_nodes(node_id),
    scale_set text NOT NULL CHECK (btrim(scale_set) <> ''),
    repository text NOT NULL CHECK (btrim(repository) <> ''),
    request_id bigint NOT NULL CHECK (request_id > 0),
    runner_name text NOT NULL CHECK (btrim(runner_name) <> ''),
    reap_requested boolean NOT NULL DEFAULT false,
    PRIMARY KEY (node_id, scale_set, repository, runner_name)
);
CREATE INDEX scaler_pending_libvirt_reaps_by_node
    ON scaler_libvirt_runner_lifecycle(node_id) WHERE reap_requested;
CREATE INDEX scaler_pending_libvirt_reaps_by_demand
    ON scaler_libvirt_runner_lifecycle(scale_set, repository, request_id) WHERE reap_requested;

---- create above / drop below ----

DROP TABLE scaler_libvirt_runner_lifecycle;
