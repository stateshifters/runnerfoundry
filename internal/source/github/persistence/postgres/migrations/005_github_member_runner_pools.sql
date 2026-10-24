SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.runnerfoundry_github_member_runner_pools (
    member_id text NOT NULL REFERENCES public.runnerfoundry_members(member_id) ON DELETE CASCADE,
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    runner_group text NOT NULL CHECK (btrim(runner_group) <> ''),
    PRIMARY KEY (member_id, runner_pool)
);

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TABLE public.runnerfoundry_github_member_runner_pools;
