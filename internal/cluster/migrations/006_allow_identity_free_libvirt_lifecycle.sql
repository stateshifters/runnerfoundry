-- RunnerFoundry nodes are stopped for this migration. Replacing the constraint
-- takes a brief ACCESS EXCLUSIVE lock and validates the retained lifecycle rows.
ALTER TABLE scaler_libvirt_runner_lifecycle
    DROP CONSTRAINT scaler_libvirt_runner_lifecycle_request_id_check;
ALTER TABLE scaler_libvirt_runner_lifecycle
    ADD CONSTRAINT scaler_libvirt_runner_lifecycle_request_id_check CHECK (request_id >= 0);

---- create above / drop below ----

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM scaler_libvirt_runner_lifecycle
        WHERE request_id = 0
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while identity-free libvirt runner lifecycle rows remain';
    END IF;
END;
$$;

ALTER TABLE scaler_libvirt_runner_lifecycle
    DROP CONSTRAINT scaler_libvirt_runner_lifecycle_request_id_check;
ALTER TABLE scaler_libvirt_runner_lifecycle
    ADD CONSTRAINT scaler_libvirt_runner_lifecycle_request_id_check CHECK (request_id > 0);
