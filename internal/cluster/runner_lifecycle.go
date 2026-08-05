package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const expireClusterRunnersSQL = `UPDATE scaler_runner_lifecycle
	SET phase = 'reap_pending', reap_reason = 'job_start_timeout', updated_at = $1
	WHERE scale_set = $2 AND ($3 = '' OR repository = $3) AND phase = 'waiting_for_job'
	  AND waiting_since <= $4`

const releaseExpiredClusterAssignmentsSQL = `UPDATE scaler_assignments SET state = 'complete'
	WHERE assignment_id IN (
		SELECT assignment_id FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND ($2 = '' OR repository = $2)
		  AND phase = 'reap_pending' AND reap_reason = 'job_start_timeout' AND updated_at = $3
	) AND state <> 'complete'`

func (store *Store) ConfirmRunnerWaiting(
	ctx context.Context,
	runner scaler.RunnerLifecycle,
	confirmedAt time.Time,
) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	commandTag, err := connection.Exec(ctx, `UPDATE scaler_runner_lifecycle
		SET phase = 'waiting_for_job', github_runner_id = $1, waiting_since = $2, updated_at = $2
		WHERE node_id = $3 AND scale_set = $4 AND repository = $5 AND runner_name = $6
		  AND phase = 'provisioning'`, runner.GitHubRunnerID, confirmedAt, runner.NodeID,
		runner.ScaleSet, runner.Repository, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("confirm clustered runner waiting: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	return nil
}

func (store *Store) CompleteRunnerLifecycleJob(
	ctx context.Context,
	completion scaler.JobCompletion,
	_ time.Time,
) error {
	return store.CompleteJob(ctx, JobCompletion{
		ScaleSet: completion.ScaleSet, Repository: completion.Repository,
		RunnerIdentity: completion.RunnerIdentity,
	})
}

func (store *Store) RequestExpiredRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
	deadline, requestedAt time.Time,
) (changed int, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return 0, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin clustered runner timeout: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	changed, err = expireClusterRunnersInTransaction(ctx, transaction, scaleSet, repository, deadline, requestedAt)
	if err != nil {
		return 0, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit clustered runner timeout: %w", err)
	}
	return changed, nil
}

func expireClusterRunnersInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	deadline, requestedAt time.Time,
) (int, error) {
	commandTag, err := transaction.Exec(ctx, expireClusterRunnersSQL, requestedAt, scaleSet, repository, deadline)
	if err != nil {
		return 0, fmt.Errorf("request clustered expired runner reaps: %w", err)
	}
	changed := int(commandTag.RowsAffected())
	if err = reconcileClusterBulkLibvirtReaps(
		ctx, transaction, scaleSet, repository, "job_start_timeout",
	); err != nil {
		return 0, err
	}
	if _, err = transaction.Exec(ctx, releaseExpiredClusterAssignmentsSQL, scaleSet, repository, requestedAt); err != nil {
		return 0, fmt.Errorf("release expired runner assignments: %w", err)
	}
	return changed, nil
}

func (store *Store) PendingRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
) ([]scaler.RunnerLifecycle, error) {
	return store.loadRunnerLifecycles(ctx, `SELECT node_id, scale_set, repository, provider, runner_name,
		request_id, COALESCE(assignment_id, 0), installation_id, github_scale_set_id, github_runner_id, phase, created_at,
		COALESCE(waiting_since, 'epoch'::timestamptz), updated_at, reap_reason
		FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND ($2 = '' OR repository = $2) AND phase = 'reap_pending'
		ORDER BY node_id, repository, runner_name`,
		"load clustered pending runner reaps", "scan clustered pending runner reap", "read clustered pending runner reaps",
		scaleSet, repository)
}

func (store *Store) PendingNodeRunnerReaps(
	ctx context.Context,
	nodeID NodeID,
) ([]scaler.RunnerLifecycle, error) {
	return store.loadRunnerLifecycles(ctx, `SELECT node_id, scale_set, repository, provider, runner_name,
		request_id, COALESCE(assignment_id, 0), installation_id, github_scale_set_id,
		github_runner_id, phase, created_at, COALESCE(waiting_since, 'epoch'::timestamptz), updated_at, reap_reason
		FROM scaler_runner_lifecycle
		WHERE node_id = $1 AND phase = 'reap_pending'
		ORDER BY scale_set, repository, runner_name`,
		"load clustered node runner reaps", "scan clustered node runner reap", "read clustered node runner reaps", nodeID)
}

func (store *Store) RunnerLifecycles(
	ctx context.Context,
	scaleSet, repository string,
) ([]scaler.RunnerLifecycle, error) {
	return store.loadRunnerLifecycles(ctx, `SELECT node_id, scale_set, repository, provider, runner_name,
		request_id, COALESCE(assignment_id, 0), installation_id, github_scale_set_id,
		github_runner_id, phase, created_at, COALESCE(waiting_since, 'epoch'::timestamptz), updated_at, reap_reason
		FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND ($2 = '' OR repository = $2)
		ORDER BY node_id, repository, runner_name`,
		"load clustered runner lifecycles", "scan clustered runner lifecycle", "read clustered runner lifecycles",
		scaleSet, repository)
}

func (store *Store) loadRunnerLifecycles(
	ctx context.Context,
	query, loadOperation, scanOperation, readOperation string,
	arguments ...any,
) (runners []scaler.RunnerLifecycle, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	rows, err := connection.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", loadOperation, err)
	}
	defer rows.Close()
	for rows.Next() {
		runner, scanErr := scanClusterRunnerLifecycle(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", scanOperation, scanErr)
		}
		runners = append(runners, runner)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", readOperation, err)
	}
	return runners, nil
}

func scanClusterRunnerLifecycle(rows *databaseRows) (scaler.RunnerLifecycle, error) {
	var runner scaler.RunnerLifecycle
	if err := rows.Scan(&runner.NodeID, &runner.ScaleSet, &runner.Repository, &runner.Provider, &runner.RunnerName,
		&runner.RequestID, &runner.AssignmentID, &runner.InstallationID, &runner.GitHubScaleSetID,
		&runner.GitHubRunnerID, &runner.Phase, &runner.CreatedAt, &runner.WaitingSince,
		&runner.UpdatedAt, &runner.ReapReason); err != nil {
		return scaler.RunnerLifecycle{}, fmt.Errorf("scan runner lifecycle row: %w", err)
	}
	if runner.WaitingSince.Equal(time.Unix(0, 0).UTC()) {
		runner.WaitingSince = time.Time{}
	}
	return runner, nil
}

func (store *Store) ConfirmRunnerReaped(ctx context.Context, runner scaler.RunnerLifecycle) (resultErr error) {
	if err := runner.Identity().Validate(); err != nil {
		return fmt.Errorf("validate clustered terminal runner identity: %w", err)
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin clustered runner reap confirmation: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = confirmClusterRunnerReapedInTransaction(ctx, transaction, runner); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit clustered runner reap confirmation: %w", err)
	}
	return nil
}

func confirmClusterRunnerReapedInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
) error {
	identity := scaler.RunnerIdentity{RequestID: runner.RequestID, RunnerName: runner.RunnerName}
	predicate, err := newClusterRunnerIdentityPredicate(identity, 4)
	if err != nil {
		return err
	}
	authoritativeRunner, err := loadClusterTerminalRunnerAuthority(ctx, transaction, runner, identity, predicate)
	if err != nil {
		return err
	}
	if err = deleteClusterRunnerLifecycle(ctx, transaction, runner, identity, predicate); err != nil {
		return err
	}
	if authoritativeRunner.Provider == scaler.RunnerProviderLibvirt {
		if err = deleteClusterLibvirtRunnerLifecycle(ctx, transaction, authoritativeRunner, identity); err != nil {
			return err
		}
	}
	return nil
}

func loadClusterTerminalRunnerAuthority(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	identity scaler.RunnerIdentity,
	predicate clusterRunnerIdentityPredicate,
) (scaler.RunnerLifecycle, error) {
	arguments := append([]any{runner.NodeID, runner.ScaleSet, runner.Repository}, predicate.arguments...)
	var authority scaler.RunnerLifecycle
	var nameConflicts, requestOwners int64
	err := transaction.QueryRow(ctx, fmt.Sprintf(clusterTerminalRunnerAuthoritySQL, predicate.condition), arguments...).Scan(
		&authority.Provider, &authority.RunnerName, &authority.RequestID, &nameConflicts, &requestOwners,
	)
	if err != nil {
		return authority, fmt.Errorf("load authoritative terminal runner lifecycle: %w", err)
	}
	authority.NodeID = runner.NodeID
	authority.ScaleSet = runner.ScaleSet
	authority.Repository = runner.Repository
	if nameConflicts > 1 || requestOwners > 1 {
		return authority, &scaler.RunnerIdentityInvariantError{
			Identity: identity, Matches: max(nameConflicts, requestOwners),
		}
	}
	if nameConflicts == 1 || requestOwners == 1 {
		return authority, clusterLifecycleConsistencyError(authority, "legacy identity conflicts with terminal reap")
	}
	if err = validateClusterTerminalProvider(ctx, transaction, authority); err != nil {
		return authority, err
	}
	return authority, nil
}

func validateClusterTerminalProvider(
	ctx context.Context,
	transaction *databaseTransaction,
	authority scaler.RunnerLifecycle,
) error {
	if authority.Provider == scaler.RunnerProviderLibvirt {
		return nil
	}
	var legacyExists bool
	err := transaction.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4)`,
		authority.NodeID, authority.ScaleSet, authority.Repository, authority.RunnerName).Scan(&legacyExists)
	if err != nil {
		return fmt.Errorf("inspect terminal legacy provider conflict: %w", err)
	}
	if legacyExists {
		return clusterLifecycleConsistencyError(authority, "legacy libvirt lifecycle conflicts with authoritative provider")
	}
	return nil
}

func deleteClusterRunnerLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	identity scaler.RunnerIdentity,
	predicate clusterRunnerIdentityPredicate,
) error {
	return deleteClusterReapedLifecycle(ctx, transaction, runner, identity, predicate,
		"scaler_runner_lifecycle", "phase = 'reap_pending'", "confirm clustered runner reaped")
}

func deleteClusterLibvirtRunnerLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	identity scaler.RunnerIdentity,
) error {
	changed, err := deleteCompatibleClusterLibvirtLifecycle(ctx, transaction, runner)
	if err != nil {
		return err
	}
	if changed > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: changed}
	}
	if changed == 1 {
		return nil
	}
	return classifyUndeletedClusterLibvirtLifecycle(ctx, transaction, runner, identity)
}

func deleteCompatibleClusterLibvirtLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
) (int64, error) {
	commandTag, err := transaction.Exec(ctx, `DELETE FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4
		  AND (request_id = 0 OR request_id = $5)
		  AND NOT EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle owner
		      WHERE owner.scale_set = $2 AND owner.repository = $3
		        AND owner.request_id = $5 AND $5 > 0 AND owner.runner_name <> $4)`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName, runner.RequestID)
	if err != nil {
		return 0, fmt.Errorf("confirm mirrored clustered libvirt runner reap: %w", err)
	}
	return commandTag.RowsAffected(), nil
}

func classifyUndeletedClusterLibvirtLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	identity scaler.RunnerIdentity,
) error {
	var nameConflicts, requestOwners int64
	if err := transaction.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE runner_name = $4),
		COUNT(*) FILTER (WHERE request_id = $5 AND $5 > 0 AND runner_name <> $4)
		FROM scaler_libvirt_runner_lifecycle
		WHERE scale_set = $2 AND repository = $3
		  AND (node_id = $1 OR (request_id = $5 AND $5 > 0 AND runner_name <> $4))`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName, runner.RequestID,
	).Scan(&nameConflicts, &requestOwners); err != nil {
		return fmt.Errorf("inspect mirrored clustered libvirt runner reap: %w", err)
	}
	if nameConflicts > 1 || requestOwners > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: max(nameConflicts, requestOwners)}
	}
	if nameConflicts == 1 || requestOwners == 1 {
		return clusterLifecycleConsistencyError(runner, "legacy request identity conflicts with terminal reap")
	}
	return nil
}

func deleteClusterReapedLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	runner scaler.RunnerLifecycle,
	identity scaler.RunnerIdentity,
	predicate clusterRunnerIdentityPredicate,
	table, eligibility, failure string,
) error {
	statement := fmt.Sprintf(`DELETE FROM %s
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND %s AND %s`,
		table, eligibility, predicate.condition)
	arguments := append([]any{runner.NodeID, runner.ScaleSet, runner.Repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("%s: %w", failure, err)
	}
	scope := clusterRunnerIdentityScope{
		table:     table,
		condition: fmt.Sprintf("node_id = $1 AND scale_set = $2 AND repository = $3 AND %s", eligibility),
		arguments: []any{runner.NodeID, runner.ScaleSet, runner.Repository},
	}
	return requireClusterRunnerIdentityMatch(ctx, transaction, scope, identity, commandTag.RowsAffected())
}

var _ scaler.RunnerLifecycleStore = (*Store)(nil)
var _ scaler.RunnerJobCompletionStore = (*Store)(nil)
