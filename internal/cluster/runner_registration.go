package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const registerClusterRunnerLifecycleSQL = `
	WITH locked AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(hashtextextended($9, 0))
	)
	INSERT INTO scaler_runner_lifecycle (
		node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
		installation_id, github_scale_set_id, github_runner_id, phase, created_at, updated_at
	)
	SELECT assignment.node_id, demand.scale_set,
	       demand.repository, $3, $4,
	       COALESCE(demand.request_id, 0),
	       assignment.assignment_id, $5, $6, $7, 'provisioning', $8, $8
	FROM scaler_assignments assignment
	JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
	LEFT JOIN scaler_assigned_job_observations observation
	  ON observation.assigned_observation_id = demand.assigned_observation_id
	CROSS JOIN locked
	WHERE assignment.assignment_id = $1 AND assignment.node_id = $2 AND assignment.state <> 'complete'
	  AND (demand.source = 'job_available' OR EXISTS (
	      SELECT 1 FROM scaler_repository_controls control
	      WHERE control.app_id = observation.app_id
	        AND control.controller_epoch = demand.controller_epoch
	        AND observation.controller_epoch = demand.controller_epoch
	  ))
	ON CONFLICT (node_id, scale_set, repository, runner_name) DO NOTHING`

func (store *Store) RegisterRunner(ctx context.Context, runner scaler.RunnerLifecycle) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin clustered runner lifecycle registration: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = registerRunnerInTransaction(ctx, transaction, runner); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit clustered runner lifecycle registration: %w", err)
	}
	return nil
}

func registerRunnerInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
) error {
	state, err := loadClusterLifecycleRegistrationState(ctx, transaction, runner)
	if err != nil {
		return err
	}
	if err = validateClusterLifecycleRegistrationState(runner, state); err != nil {
		return err
	}
	if err = ensureClusterGenericRegistration(ctx, transaction, runner, state); err != nil {
		return err
	}
	if runner.Provider != scaler.RunnerProviderLibvirt {
		return nil
	}
	if err = reconcileClusterLibvirtRegistration(ctx, transaction, runner); err != nil {
		return err
	}
	state, err = loadClusterLifecycleRegistrationState(ctx, transaction, runner)
	if err != nil {
		return err
	}
	return validateReconciledClusterLifecycleRegistration(runner, state)
}

func ensureClusterGenericRegistration(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	state clusterLifecycleRegistrationState,
) error {
	if state.genericExists {
		return nil
	}
	if err := insertClusterRunnerLifecycle(ctx, transaction, runner); err != nil {
		return err
	}
	state, err := loadClusterLifecycleRegistrationState(ctx, transaction, runner)
	if err != nil {
		return err
	}
	if err = validateClusterLifecycleRegistrationState(runner, state); err != nil {
		return err
	}
	if !state.genericExists {
		return &AssignmentTransitionError{
			AssignmentID: AssignmentID(runner.AssignmentID), Expected: AssignmentStarting,
		}
	}
	return nil
}

func validateReconciledClusterLifecycleRegistration(
	runner scaler.RunnerLifecycle,
	state clusterLifecycleRegistrationState,
) error {
	if err := validateClusterLifecycleRegistrationState(runner, state); err != nil {
		return err
	}
	if !state.mirrorExists || state.mirrorReap != (runner.Phase == scaler.RunnerReapPending) {
		return clusterLifecycleConsistencyError(runner, "legacy lifecycle could not be reconciled")
	}
	return nil
}

func insertClusterRunnerLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
) error {
	commandTag, err := transaction.Exec(ctx, registerClusterRunnerLifecycleSQL,
		runner.AssignmentID, runner.NodeID, runner.Provider, runner.RunnerName,
		runner.InstallationID, runner.GitHubScaleSetID, runner.GitHubRunnerID, runner.CreatedAt,
		clusterLifecycleAdvisoryKey(runner.ScaleSet, runner.Repository, runner.Identity()))
	if err != nil {
		return fmt.Errorf("register clustered runner lifecycle: %w", err)
	}
	if commandTag.RowsAffected() > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: runner.Identity(), Matches: commandTag.RowsAffected()}
	}
	return nil
}

type clusterLifecycleRegistrationState struct {
	genericExists bool
	provider      scaler.RunnerProvider
	requestID     int64
	assignmentID  int64
	phase         scaler.RunnerLifecyclePhase
	mirrorExists  bool
	mirrorRequest int64
	mirrorReap    bool
	genericOwners int64
	mirrorOwners  int64
}

func loadClusterLifecycleRegistrationState(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
) (clusterLifecycleRegistrationState, error) {
	var state clusterLifecycleRegistrationState
	if err := loadClusterGenericRegistrationState(ctx, transaction, runner, &state); err != nil {
		return state, err
	}
	if err := loadClusterGenericRequestOwners(ctx, transaction, runner, &state); err != nil {
		return state, err
	}
	if err := loadClusterMirrorRegistrationState(ctx, transaction, runner, &state); err != nil {
		return state, err
	}
	if err := loadClusterMirrorRequestOwners(ctx, transaction, runner, &state); err != nil {
		return state, err
	}
	return state, nil
}

func loadClusterGenericRequestOwners(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	state *clusterLifecycleRegistrationState,
) error {
	if runner.RequestID <= 0 {
		return nil
	}
	err := transaction.QueryRow(ctx, `SELECT COUNT(*) FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND repository = $2 AND request_id = $3
		  AND runner_name <> $4`, runner.ScaleSet, runner.Repository,
		runner.RequestID, runner.RunnerName).Scan(&state.genericOwners)
	if err != nil {
		return fmt.Errorf("inspect clustered authoritative request ownership: %w", err)
	}
	return nil
}

func loadClusterGenericRegistrationState(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	state *clusterLifecycleRegistrationState,
) error {
	err := transaction.QueryRow(ctx, `SELECT provider, request_id, COALESCE(assignment_id, 0), phase
		FROM scaler_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName,
	).Scan(&state.provider, &state.requestID, &state.assignmentID, &state.phase)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("inspect clustered runner lifecycle registration: %w", err)
	}
	state.genericExists = err == nil
	return nil
}

func loadClusterMirrorRegistrationState(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	state *clusterLifecycleRegistrationState,
) error {
	err := transaction.QueryRow(ctx, `SELECT request_id, reap_requested
		FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName,
	).Scan(&state.mirrorRequest, &state.mirrorReap)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("inspect clustered mirrored runner lifecycle registration: %w", err)
	}
	state.mirrorExists = err == nil
	return nil
}

func loadClusterMirrorRequestOwners(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	state *clusterLifecycleRegistrationState,
) error {
	if runner.RequestID <= 0 {
		return nil
	}
	err := transaction.QueryRow(ctx, `SELECT COUNT(*) FROM scaler_libvirt_runner_lifecycle
		WHERE scale_set = $1 AND repository = $2 AND request_id = $3
		  AND runner_name <> $4`, runner.ScaleSet, runner.Repository,
		runner.RequestID, runner.RunnerName).Scan(&state.mirrorOwners)
	if err != nil {
		return fmt.Errorf("inspect clustered legacy request ownership: %w", err)
	}
	return nil
}

func validateClusterLifecycleRegistrationState(
	runner scaler.RunnerLifecycle,
	state clusterLifecycleRegistrationState,
) error {
	if err := validateClusterGenericRegistrationState(runner, state); err != nil {
		return err
	}
	return validateClusterMirrorRegistrationState(runner, state)
}

func validateClusterGenericRegistrationState(
	runner scaler.RunnerLifecycle,
	state clusterLifecycleRegistrationState,
) error {
	if state.genericOwners > 0 {
		return clusterLifecycleConsistencyError(runner, "authoritative request identity belongs to another runner name")
	}
	if state.genericExists && state.provider != runner.Provider {
		return clusterLifecycleConsistencyError(runner, "authoritative provider conflicts with registration")
	}
	if state.genericExists && (state.requestID != runner.RequestID || state.assignmentID != runner.AssignmentID) {
		return clusterLifecycleConsistencyError(runner, "authoritative identity conflicts with registration")
	}
	if state.genericExists && state.phase == scaler.RunnerReapPending && runner.Phase != scaler.RunnerReapPending {
		return clusterLifecycleConsistencyError(runner, "authoritative lifecycle is pending reap")
	}
	return nil
}

func validateClusterMirrorRegistrationState(
	runner scaler.RunnerLifecycle,
	state clusterLifecycleRegistrationState,
) error {
	if state.mirrorExists && runner.Provider != scaler.RunnerProviderLibvirt {
		return clusterLifecycleConsistencyError(runner, "legacy libvirt lifecycle conflicts with authoritative provider")
	}
	if state.mirrorExists && state.mirrorRequest > 0 && state.mirrorRequest != runner.RequestID {
		return clusterLifecycleConsistencyError(runner, "legacy request identity conflicts with authoritative lifecycle")
	}
	if state.mirrorOwners > 0 {
		return clusterLifecycleConsistencyError(runner, "legacy request identity belongs to another runner name")
	}
	return nil
}

func clusterLifecycleConsistencyError(runner scaler.RunnerLifecycle, reason string) error {
	return &scaler.RunnerLifecycleConsistencyError{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName, Reason: reason,
	}
}

func reconcileClusterLibvirtRegistration(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
) error {
	_, err := transaction.Exec(ctx, `WITH lifecycle AS MATERIALIZED (
		SELECT $1::text AS node_id, $2::text AS scale_set, $3::text AS repository,
		       $4::bigint AS request_id, $5::text AS runner_name, $6::boolean AS reap_requested
	), locked AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(hashtextextended(concat_ws(E'\x1f', scale_set, repository,
			CASE WHEN request_id > 0 THEN request_id::text ELSE runner_name END), 0)) FROM lifecycle
	)
	INSERT INTO scaler_libvirt_runner_lifecycle AS legacy
		(node_id, scale_set, repository, request_id, runner_name, reap_requested)
		SELECT node_id, scale_set, repository, request_id, runner_name, reap_requested FROM lifecycle CROSS JOIN locked
		WHERE NOT EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle owner
			WHERE owner.scale_set = lifecycle.scale_set AND owner.repository = lifecycle.repository
			  AND owner.request_id = lifecycle.request_id
			  AND lifecycle.request_id > 0 AND owner.runner_name <> lifecycle.runner_name)
		ON CONFLICT (node_id, scale_set, repository, runner_name) DO UPDATE SET
		request_id = EXCLUDED.request_id, reap_requested = EXCLUDED.reap_requested
		WHERE legacy.request_id = 0 OR legacy.request_id = EXCLUDED.request_id`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName,
		runner.Phase == scaler.RunnerReapPending)
	if err != nil {
		return fmt.Errorf("reconcile clustered libvirt lifecycle: %w", err)
	}
	return nil
}
