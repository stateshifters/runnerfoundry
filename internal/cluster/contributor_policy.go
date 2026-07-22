package cluster

const contributorEligibilitySQL = `
	WITH live_contributions AS (
	    SELECT contribution.node_id,
	           contribution.scale_set,
	           contribution.runner_group,
	           contribution.max_runners
	    FROM scaler_contributions contribution
	    JOIN scaler_nodes node ON node.node_id = contribution.node_id
	    WHERE contribution.healthy
	      AND node.healthy
	      AND node.heartbeat_at >= $1
	),
	scale_set_compatibility AS (
	    SELECT scale_set,
	           COUNT(DISTINCT runner_group) <= 1 AS compatible
	    FROM live_contributions
	    GROUP BY scale_set
	),
	eligible_contributions AS (
	    SELECT live_contribution.node_id,
	           live_contribution.scale_set,
	           live_contribution.runner_group,
	           live_contribution.max_runners
	    FROM live_contributions live_contribution
	    JOIN scale_set_compatibility compatibility
	      ON compatibility.scale_set = live_contribution.scale_set
	    WHERE compatibility.compatible
	)`
