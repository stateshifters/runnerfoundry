CREATE TABLE scaler_repository_controls (
    app_id bigint PRIMARY KEY CHECK (app_id > 0),
    discovery_generation bigint NOT NULL DEFAULT 0 CHECK (discovery_generation >= 0),
    last_successful_discovery timestamptz,
    controller_epoch bigint NOT NULL DEFAULT 0 CHECK (controller_epoch >= 0),
    initialized boolean NOT NULL DEFAULT false
);

CREATE TABLE scaler_installations (
    app_id bigint NOT NULL REFERENCES scaler_repository_controls(app_id) ON DELETE CASCADE,
    installation_id bigint NOT NULL CHECK (installation_id > 0),
    last_seen_generation bigint NOT NULL CHECK (last_seen_generation > 0),
    PRIMARY KEY (app_id, installation_id)
);

CREATE TABLE scaler_repositories (
    app_id bigint NOT NULL,
    repository_url text NOT NULL CHECK (btrim(repository_url) <> ''),
    installation_id bigint NOT NULL,
    last_seen_generation bigint NOT NULL CHECK (last_seen_generation > 0),
    PRIMARY KEY (app_id, repository_url),
    FOREIGN KEY (app_id, installation_id)
        REFERENCES scaler_installations(app_id, installation_id) ON DELETE CASCADE
);
CREATE INDEX scaler_repositories_by_installation_generation
    ON scaler_repositories(app_id, installation_id, last_seen_generation);

CREATE TABLE scaler_registrations (
    app_id bigint NOT NULL,
    repository_url text NOT NULL,
    scale_set_name text NOT NULL CHECK (btrim(scale_set_name) <> ''),
    runner_group text NOT NULL CHECK (btrim(runner_group) <> ''),
    github_scale_set_id integer NOT NULL CHECK (github_scale_set_id > 0),
    PRIMARY KEY (app_id, repository_url, scale_set_name),
    FOREIGN KEY (app_id, repository_url)
        REFERENCES scaler_repositories(app_id, repository_url) ON DELETE CASCADE
);

---- create above / drop below ----

DROP TABLE scaler_registrations;
DROP INDEX scaler_repositories_by_installation_generation;
DROP TABLE scaler_repositories;
DROP TABLE scaler_installations;
DROP TABLE scaler_repository_controls;
