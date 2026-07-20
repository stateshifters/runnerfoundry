CREATE TABLE scaler_schema_compatibility (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    oldest_binary_schema_generation integer NOT NULL CHECK (oldest_binary_schema_generation >= 0)
);
INSERT INTO scaler_schema_compatibility (oldest_binary_schema_generation) VALUES (1);

CREATE TABLE scaler_nodes (
    node_id text PRIMARY KEY CHECK (btrim(node_id) <> ''),
    heartbeat_at timestamptz NOT NULL,
    healthy boolean NOT NULL
);
CREATE TABLE scaler_contributions (
    node_id text NOT NULL REFERENCES scaler_nodes(node_id) ON DELETE CASCADE,
    scale_set text NOT NULL CHECK (btrim(scale_set) <> ''),
    runner_group text NOT NULL CHECK (btrim(runner_group) <> ''),
    max_runners integer NOT NULL CHECK (max_runners >= 0),
    healthy boolean NOT NULL,
    PRIMARY KEY (node_id, scale_set)
);
CREATE TABLE scaler_demand (
    demand_id bigserial PRIMARY KEY,
    observation_id bigserial UNIQUE NOT NULL,
    scale_set text NOT NULL CHECK (btrim(scale_set) <> ''),
    repository text NOT NULL CHECK (btrim(repository) <> ''),
    request_id bigint NOT NULL CHECK (request_id > 0),
    queue_time timestamptz NOT NULL,
    UNIQUE (scale_set, repository, request_id)
);
CREATE TABLE scaler_assignments (
    assignment_id bigserial PRIMARY KEY,
    demand_id bigint NOT NULL REFERENCES scaler_demand(demand_id) ON DELETE CASCADE,
    node_id text NOT NULL REFERENCES scaler_nodes(node_id),
    installation_id bigint CHECK (installation_id > 0),
    github_scale_set_id integer CHECK (github_scale_set_id > 0),
    state text NOT NULL CHECK (state IN ('reserved', 'acquiring', 'ready', 'starting', 'running', 'complete')),
    CHECK (
        state IN ('reserved', 'complete')
        OR (installation_id IS NOT NULL AND github_scale_set_id IS NOT NULL)
    )
);
CREATE UNIQUE INDEX scaler_one_active_assignment_per_demand
    ON scaler_assignments(demand_id) WHERE state <> 'complete';
CREATE INDEX scaler_fifo_demand
    ON scaler_demand(scale_set, queue_time, observation_id);
CREATE INDEX scaler_active_assignments_by_node
    ON scaler_assignments(node_id) WHERE state <> 'complete';

---- create above / drop below ----

DROP TABLE scaler_assignments;
DROP TABLE scaler_demand;
DROP TABLE scaler_contributions;
DROP TABLE scaler_nodes;
DROP TABLE scaler_schema_compatibility;
