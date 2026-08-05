CREATE TABLE public.runnerfoundry_members (
    member_id uuid PRIMARY KEY,
    heartbeat_at timestamptz NOT NULL
);

CREATE INDEX runnerfoundry_members_heartbeat_idx
    ON public.runnerfoundry_members (heartbeat_at);

CREATE TABLE public.runnerfoundry_member_runner_pools (
    member_id uuid NOT NULL
        REFERENCES public.runnerfoundry_members (member_id) ON DELETE CASCADE,
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    capacity integer NOT NULL CHECK (capacity > 0),
    PRIMARY KEY (member_id, runner_pool)
);

CREATE TABLE public.runnerfoundry_jobs (
    job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name text NOT NULL CHECK (btrim(source_name) <> ''),
    source_reference text NOT NULL CHECK (btrim(source_reference) <> ''),
    runner_pool text NOT NULL CHECK (btrim(runner_pool) <> ''),
    state text NOT NULL
        CONSTRAINT runnerfoundry_jobs_state_value_check
        CHECK (state IN ('queued', 'attempting', 'result_pending')),
    enqueued_at timestamptz NOT NULL,
    member_id uuid REFERENCES public.runnerfoundry_members (member_id),
    claimed_at timestamptz,
    acquisition_started_at timestamptz,
    acquired_at timestamptz,
    started_at timestamptz,
    result text CHECK (result IN ('success', 'failure')),
    result_origin text CHECK (result_origin IN ('runner', 'service')),
    result_reason text,
    UNIQUE (source_name, source_reference),
    CONSTRAINT runnerfoundry_jobs_state_check CHECK (
        (state = 'queued'
            AND member_id IS NULL
            AND claimed_at IS NULL
            AND acquisition_started_at IS NULL
            AND acquired_at IS NULL
            AND started_at IS NULL
            AND result IS NULL
            AND result_origin IS NULL
            AND result_reason IS NULL)
        OR
        (state = 'attempting'
            AND member_id IS NOT NULL
            AND claimed_at IS NOT NULL
            AND (acquired_at IS NULL OR acquisition_started_at IS NOT NULL)
            AND result IS NULL
            AND result_origin IS NULL
            AND result_reason IS NULL)
        OR
        (state = 'result_pending'
            AND result IS NOT NULL
            AND result_origin IS NOT NULL
            AND (
                (member_id IS NULL
                    AND claimed_at IS NULL
                    AND acquisition_started_at IS NULL
                    AND acquired_at IS NULL
                    AND started_at IS NULL)
                OR
                (member_id IS NOT NULL AND claimed_at IS NOT NULL)
            ))
    ),
    CONSTRAINT runnerfoundry_jobs_acquisition_order_check CHECK (
        (acquisition_started_at IS NULL
            OR (member_id IS NOT NULL
                AND claimed_at IS NOT NULL
                AND acquisition_started_at >= claimed_at))
        AND (acquired_at IS NULL
            OR (acquisition_started_at IS NOT NULL AND acquired_at >= acquisition_started_at))
    ),
    CONSTRAINT runnerfoundry_jobs_started_ownership_check CHECK (
        started_at IS NULL OR (member_id IS NOT NULL AND claimed_at IS NOT NULL)
    )
);

CREATE INDEX runnerfoundry_jobs_fifo_idx
    ON public.runnerfoundry_jobs (runner_pool, enqueued_at)
    WHERE state = 'queued';

CREATE INDEX runnerfoundry_jobs_member_ownership_idx
    ON public.runnerfoundry_jobs (member_id, runner_pool, claimed_at)
    WHERE member_id IS NOT NULL;

CREATE INDEX runnerfoundry_jobs_pending_acquisition_idx
    ON public.runnerfoundry_jobs (claimed_at)
    WHERE state = 'attempting' AND acquired_at IS NULL;

CREATE INDEX runnerfoundry_jobs_pending_result_idx
    ON public.runnerfoundry_jobs (enqueued_at)
    WHERE state = 'result_pending';

CREATE TABLE public.runnerfoundry_source_receipts (
    source_name text NOT NULL CHECK (btrim(source_name) <> ''),
    source_reference text NOT NULL CHECK (btrim(source_reference) <> ''),
    result text NOT NULL CHECK (result IN ('success', 'failure')),
    result_origin text NOT NULL CHECK (result_origin IN ('runner', 'service')),
    delivery text NOT NULL CHECK (delivery IN ('reported', 'runner_managed', 'unsupported')),
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
DROP TABLE public.runnerfoundry_source_receipts;
DROP TABLE public.runnerfoundry_jobs;
DROP TABLE public.runnerfoundry_member_runner_pools;
DROP TABLE public.runnerfoundry_members;
