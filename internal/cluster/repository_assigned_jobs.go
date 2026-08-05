package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
)

const requestRemovedInstallationAssignedRunnerReapsSQL = `UPDATE scaler_runner_lifecycle lifecycle
	SET phase = 'reap_pending', reap_reason = 'installation_withdrawn', updated_at = $3
	WHERE lifecycle.assignment_id IN (SELECT assignment.assignment_id
		FROM scaler_assignments assignment
		JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		JOIN scaler_assigned_job_observations observation
		  ON observation.assigned_observation_id = demand.assigned_observation_id
		JOIN scaler_repositories repository
		  ON repository.app_id = observation.app_id AND repository.repository_url = observation.repository_url
		JOIN scaler_installations installation
		  ON installation.app_id = repository.app_id AND installation.installation_id = repository.installation_id
		WHERE installation.app_id = $1 AND installation.last_seen_generation < $2)
	  AND lifecycle.phase <> 'reap_pending'`

const completeRemovedInstallationAssignmentsSQL = `UPDATE scaler_assignments assignment SET state = 'complete'
	FROM scaler_demand demand
	WHERE demand.demand_id = assignment.demand_id
	  AND demand.assigned_observation_id IN (SELECT observation.assigned_observation_id
		FROM scaler_assigned_job_observations observation
		JOIN scaler_repositories repository
		  ON repository.app_id = observation.app_id AND repository.repository_url = observation.repository_url
		JOIN scaler_installations installation
		  ON installation.app_id = repository.app_id AND installation.installation_id = repository.installation_id
		WHERE installation.app_id = $1 AND installation.last_seen_generation < $2)
	  AND assignment.state <> 'complete'`

const removeRemovedInstallationAssignedObservationsSQL = `DELETE FROM scaler_assigned_job_observations observation
	USING scaler_repositories repository, scaler_installations installation
	WHERE repository.app_id = observation.app_id AND repository.repository_url = observation.repository_url
	  AND installation.app_id = repository.app_id AND installation.installation_id = repository.installation_id
	  AND installation.app_id = $1 AND installation.last_seen_generation < $2`

func withdrawRemovedInstallationAssignedObservations(
	ctx context.Context,
	transaction *databaseTransaction,
	update postgreSQLDiscoveryUpdate,
) error {
	if _, err := transaction.Exec(ctx, requestRemovedInstallationAssignedRunnerReapsSQL,
		update.appID, update.generation, update.completedAt); err != nil {
		return fmt.Errorf("request removed-installation assigned runner reaps: %w", err)
	}
	if _, err := transaction.Exec(ctx, completeRemovedInstallationAssignmentsSQL,
		update.appID, update.generation); err != nil {
		return fmt.Errorf("complete removed-installation assigned assignments: %w", err)
	}
	if _, err := transaction.Exec(ctx, removeRemovedInstallationAssignedObservationsSQL,
		update.appID, update.generation); err != nil {
		return fmt.Errorf("remove removed-installation assigned observations: %w", err)
	}
	return nil
}

func withdrawUndiscoveredAssignedObservations(
	ctx context.Context,
	transaction *databaseTransaction,
	appID int64,
	installationID githubapp.InstallationID,
	generation int64,
	withdrawnAt time.Time,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_runner_lifecycle lifecycle
		SET phase = 'reap_pending', reap_reason = 'repository_withdrawn', updated_at = $4
		WHERE lifecycle.assignment_id IN (SELECT assignment.assignment_id
			FROM scaler_assignments assignment
			JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
			JOIN scaler_assigned_job_observations observation
			  ON observation.assigned_observation_id = demand.assigned_observation_id
			JOIN scaler_repositories repository
			  ON repository.app_id = observation.app_id AND repository.repository_url = observation.repository_url
			WHERE repository.app_id = $1 AND repository.installation_id = $2
			  AND repository.last_seen_generation < $3)
		  AND lifecycle.phase <> 'reap_pending'`, appID, installationID, generation, withdrawnAt); err != nil {
		return fmt.Errorf("request undiscovered assigned runner reaps: %w", err)
	}
	return removeUndiscoveredAssignedObservations(ctx, transaction, appID, installationID, generation)
}

func removeUndiscoveredAssignedObservations(
	ctx context.Context,
	transaction *databaseTransaction,
	appID int64,
	installationID githubapp.InstallationID,
	generation int64,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_assignments assignment SET state = 'complete'
		FROM scaler_demand demand
		WHERE demand.demand_id = assignment.demand_id
		  AND demand.assigned_observation_id IN (SELECT observation.assigned_observation_id
			FROM scaler_assigned_job_observations observation
			JOIN scaler_repositories repository
			  ON repository.app_id = observation.app_id AND repository.repository_url = observation.repository_url
			WHERE repository.app_id = $1 AND repository.installation_id = $2
			  AND repository.last_seen_generation < $3)
		  AND assignment.state <> 'complete'`, appID, installationID, generation); err != nil {
		return fmt.Errorf("complete undiscovered assigned assignments: %w", err)
	}
	if _, err := transaction.Exec(ctx, `DELETE FROM scaler_assigned_job_observations observation
		USING scaler_repositories repository
		WHERE repository.app_id = observation.app_id AND repository.repository_url = observation.repository_url
		  AND repository.app_id = $1 AND repository.installation_id = $2
		  AND repository.last_seen_generation < $3`, appID, installationID, generation); err != nil {
		return fmt.Errorf("remove undiscovered assigned observations: %w", err)
	}
	return nil
}

func withdrawRegistrationAssignedObservation(
	ctx context.Context,
	transaction *databaseTransaction,
	appID int64,
	repositoryURL string,
	scaleSetName string,
	withdrawnAt time.Time,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_runner_lifecycle lifecycle
		SET phase = 'reap_pending', reap_reason = 'registration_withdrawn', updated_at = $1
		WHERE lifecycle.assignment_id IN (
			SELECT assignment.assignment_id FROM scaler_assignments assignment
			JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
			JOIN scaler_assigned_job_observations observation
			  ON observation.assigned_observation_id = demand.assigned_observation_id
			WHERE observation.app_id = $2 AND observation.repository_url = $3 AND observation.scale_set = $4
		) AND lifecycle.phase <> 'reap_pending'`, withdrawnAt, appID, repositoryURL, scaleSetName); err != nil {
		return fmt.Errorf("request withdrawn assigned runner reaps: %w", err)
	}
	if _, err := transaction.Exec(ctx, `UPDATE scaler_assignments assignment SET state = 'complete'
		FROM scaler_demand demand
		WHERE demand.demand_id = assignment.demand_id
		  AND demand.assigned_observation_id IN (SELECT assigned_observation_id FROM scaler_assigned_job_observations
			WHERE app_id = $1 AND repository_url = $2 AND scale_set = $3)
		  AND assignment.state <> 'complete'`, appID, repositoryURL, scaleSetName); err != nil {
		return fmt.Errorf("complete withdrawn assigned assignments: %w", err)
	}
	if _, err := transaction.Exec(ctx, `DELETE FROM scaler_assigned_job_observations
		WHERE app_id = $1 AND repository_url = $2 AND scale_set = $3`, appID, repositoryURL, scaleSetName); err != nil {
		return fmt.Errorf("remove withdrawn assigned observations: %w", err)
	}
	return nil
}
