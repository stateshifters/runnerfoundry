CREATE TABLE public.runnerfoundry_github_registrations (
    repository_url text NOT NULL CHECK (btrim(repository_url) <> ''),
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    installation_id bigint NOT NULL CHECK (installation_id > 0),
    runner_group_id bigint NOT NULL CHECK (runner_group_id > 0),
    scale_set_id bigint NOT NULL CHECK (scale_set_id > 0),
    PRIMARY KEY (repository_url, runner_pool)
);

---- create above / drop below ----

DROP TABLE public.runnerfoundry_github_registrations;
