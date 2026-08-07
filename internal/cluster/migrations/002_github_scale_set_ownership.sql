SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.runnerfoundry_github_scale_set_ownership (
    repository_url text NOT NULL CHECK (btrim(repository_url) <> ''),
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    installation_id bigint NOT NULL CHECK (installation_id > 0),
    runner_group_id bigint NOT NULL CHECK (runner_group_id > 0),
    scale_set_id bigint NOT NULL CHECK (scale_set_id > 0),
    PRIMARY KEY (repository_url, scale_set_id)
);

INSERT INTO public.runnerfoundry_github_scale_set_ownership
    (repository_url, runner_pool, installation_id, runner_group_id, scale_set_id)
SELECT repository_url, runner_pool, installation_id, runner_group_id, scale_set_id
FROM public.runnerfoundry_github_registrations;

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TABLE public.runnerfoundry_github_scale_set_ownership;
