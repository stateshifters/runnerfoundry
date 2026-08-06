CREATE TABLE public.runnerfoundry_members (
    member_id text PRIMARY KEY CHECK (btrim(member_id) <> ''),
    heartbeat_at timestamptz NOT NULL
);

CREATE INDEX runnerfoundry_members_heartbeat_idx
    ON public.runnerfoundry_members (heartbeat_at);

CREATE TABLE public.runnerfoundry_member_runner_pools (
    member_id text NOT NULL
        REFERENCES public.runnerfoundry_members (member_id) ON DELETE CASCADE,
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    capacity integer NOT NULL CHECK (capacity > 0),
    PRIMARY KEY (member_id, runner_pool)
);

CREATE TABLE public.runnerfoundry_queued_jobs (
    job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name text NOT NULL CHECK (btrim(source_name) <> ''),
    source_reference text NOT NULL CHECK (btrim(source_reference) <> ''),
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_name, source_reference)
);

CREATE INDEX runnerfoundry_queued_jobs_fifo_idx
    ON public.runnerfoundry_queued_jobs (runner_pool, enqueued_at);

CREATE TABLE public.runnerfoundry_processing_jobs (
    job_id uuid PRIMARY KEY,
    source_name text NOT NULL CHECK (btrim(source_name) <> ''),
    source_reference text NOT NULL CHECK (btrim(source_reference) <> ''),
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    member_id text NOT NULL
        REFERENCES public.runnerfoundry_members (member_id),
    claimed_at timestamptz NOT NULL,
    inventory_endpoint_id text NOT NULL CHECK (btrim(inventory_endpoint_id) <> ''),
    runner_started boolean NOT NULL DEFAULT false,
    UNIQUE (source_name, source_reference)
);

CREATE INDEX runnerfoundry_processing_jobs_member_idx
    ON public.runnerfoundry_processing_jobs (member_id, runner_pool);

CREATE INDEX runnerfoundry_processing_jobs_pool_idx
    ON public.runnerfoundry_processing_jobs (runner_pool);

CREATE TABLE public.runnerfoundry_source_tombstones (
    source_name text NOT NULL CHECK (btrim(source_name) <> ''),
    source_reference text NOT NULL CHECK (btrim(source_reference) <> ''),
    PRIMARY KEY (source_name, source_reference)
);

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
DROP TABLE public.runnerfoundry_source_tombstones;
DROP TABLE public.runnerfoundry_processing_jobs;
DROP TABLE public.runnerfoundry_queued_jobs;
DROP TABLE public.runnerfoundry_member_runner_pools;
DROP TABLE public.runnerfoundry_members;
