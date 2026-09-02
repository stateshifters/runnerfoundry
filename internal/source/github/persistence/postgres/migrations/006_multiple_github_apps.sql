SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.runnerfoundry_github_registrations_v005
    (LIKE public.runnerfoundry_github_registrations INCLUDING ALL);
ALTER TABLE public.runnerfoundry_github_registrations_v005 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.runnerfoundry_github_registrations_v005 FORCE ROW LEVEL SECURITY;
CREATE POLICY runnerfoundry_github_registrations_v005_access
    ON public.runnerfoundry_github_registrations_v005
    USING (true) WITH CHECK (true);
INSERT INTO public.runnerfoundry_github_registrations_v005
SELECT * FROM public.runnerfoundry_github_registrations;

CREATE TABLE public.runnerfoundry_github_scale_set_ownership_v005
    (LIKE public.runnerfoundry_github_scale_set_ownership INCLUDING ALL);
ALTER TABLE public.runnerfoundry_github_scale_set_ownership_v005 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.runnerfoundry_github_scale_set_ownership_v005 FORCE ROW LEVEL SECURITY;
CREATE POLICY runnerfoundry_github_scale_set_ownership_v005_access
    ON public.runnerfoundry_github_scale_set_ownership_v005
    USING (true) WITH CHECK (true);
INSERT INTO public.runnerfoundry_github_scale_set_ownership_v005
SELECT * FROM public.runnerfoundry_github_scale_set_ownership;

TRUNCATE public.runnerfoundry_github_registrations;
TRUNCATE public.runnerfoundry_github_scale_set_ownership;

ALTER TABLE public.runnerfoundry_github_registrations
    ADD COLUMN github_app_id bigint NOT NULL CHECK (github_app_id > 0);
ALTER TABLE public.runnerfoundry_github_scale_set_ownership
    ADD COLUMN github_app_id bigint NOT NULL CHECK (github_app_id > 0);

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

TRUNCATE public.runnerfoundry_github_registrations;
TRUNCATE public.runnerfoundry_github_scale_set_ownership;

ALTER TABLE public.runnerfoundry_github_registrations DROP COLUMN github_app_id;
ALTER TABLE public.runnerfoundry_github_scale_set_ownership DROP COLUMN github_app_id;

INSERT INTO public.runnerfoundry_github_registrations
SELECT * FROM public.runnerfoundry_github_registrations_v005;
INSERT INTO public.runnerfoundry_github_scale_set_ownership
SELECT * FROM public.runnerfoundry_github_scale_set_ownership_v005;

DROP TABLE public.runnerfoundry_github_scale_set_ownership_v005;
DROP TABLE public.runnerfoundry_github_registrations_v005;
