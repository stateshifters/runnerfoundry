package cluster

const reconcileClusterLibvirtLifecycleSQL = `WITH lifecycle AS MATERIALIZED (
	SELECT node_id, scale_set, repository, request_id, runner_name, phase
	FROM scaler_runner_lifecycle WHERE scale_set = $1 AND repository = $2
	  AND provider = 'libvirt' AND %s
), locked AS MATERIALIZED (
	SELECT pg_advisory_xact_lock(hashtextextended(concat_ws(E'\x1f', scale_set, repository,
		CASE WHEN request_id > 0 THEN request_id::text ELSE runner_name END), 0)) FROM lifecycle
)
INSERT INTO scaler_libvirt_runner_lifecycle AS legacy
	(node_id, scale_set, repository, request_id, runner_name, reap_requested)
	SELECT lifecycle.node_id, lifecycle.scale_set, lifecycle.repository, lifecycle.request_id,
	       lifecycle.runner_name, lifecycle.phase = 'reap_pending' FROM lifecycle CROSS JOIN locked
	WHERE true
	  AND NOT EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle owner
	      WHERE owner.scale_set = lifecycle.scale_set
	        AND owner.repository = lifecycle.repository
	        AND owner.request_id = lifecycle.request_id
	        AND owner.request_id > 0
	        AND owner.runner_name <> lifecycle.runner_name)
	ON CONFLICT (node_id, scale_set, repository, runner_name) DO UPDATE SET
	request_id = EXCLUDED.request_id, reap_requested = EXCLUDED.reap_requested
	WHERE legacy.request_id = 0 OR legacy.request_id = EXCLUDED.request_id`

const clusterTerminalRunnerAuthoritySQL = `/* cluster_terminal_lifecycle_authority */
	SELECT lifecycle.provider, lifecycle.runner_name, lifecycle.request_id,
	(SELECT COUNT(*) FROM scaler_libvirt_runner_lifecycle legacy
	 WHERE legacy.node_id = lifecycle.node_id AND legacy.scale_set = lifecycle.scale_set
	   AND legacy.repository = lifecycle.repository AND legacy.runner_name = lifecycle.runner_name
	   AND legacy.request_id NOT IN (0, lifecycle.request_id)),
	(SELECT COUNT(*) FROM scaler_libvirt_runner_lifecycle owner
	 WHERE owner.scale_set = lifecycle.scale_set AND owner.repository = lifecycle.repository
	   AND owner.request_id = lifecycle.request_id
	   AND owner.request_id > 0 AND owner.runner_name <> lifecycle.runner_name)
	FROM scaler_runner_lifecycle lifecycle
	WHERE lifecycle.node_id = $1 AND lifecycle.scale_set = $2 AND lifecycle.repository = $3
	  AND lifecycle.phase = 'reap_pending' AND %s`
