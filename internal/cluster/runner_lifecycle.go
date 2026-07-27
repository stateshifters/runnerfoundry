package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const registerClusterRunnerLifecycleSQL = `
	INSERT INTO scaler_runner_lifecycle (
		node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
		installation_id, github_scale_set_id, github_runner_id, phase, created_at, updated_at
	)
	SELECT assignment.node_id, demand.scale_set, demand.repository, $3, $4, demand.request_id,
	       assignment.assignment_id, $5, $6, $7, 'provisioning', $8, $8
	FROM scaler_assignments assignment
	JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
	WHERE assignment.assignment_id = $1 AND assignment.node_id = $2 AND assignment.state <> 'complete'
	ON CONFLICT (node_id, scale_set, repository, runner_name) DO NOTHING`

const verifyClusterRunnerLifecycleSQL = `SELECT EXISTS (
	SELECT 1 FROM scaler_runner_lifecycle
	WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4
	  AND provider = $5 AND request_id = $6 AND assignment_id = $7 AND phase <> 'reap_pending'
)`

const expireClusterRunnersSQL = `UPDATE scaler_runner_lifecycle
	SET phase = 'reap_pending', reap_reason = 'job_start_timeout', updated_at = $1
	WHERE scale_set = $2 AND ($3 = '' OR repository = $3) AND phase = 'waiting_for_job'
	  AND waiting_since <= $4`

const mirrorExpiredClusterRunnersSQL = `UPDATE scaler_libvirt_runner_lifecycle legacy SET reap_requested = true
	WHERE EXISTS (SELECT 1 FROM scaler_runner_lifecycle lifecycle
		WHERE lifecycle.node_id = legacy.node_id AND lifecycle.scale_set = legacy.scale_set
		  AND lifecycle.repository = legacy.repository AND lifecycle.runner_name = legacy.runner_name
		  AND lifecycle.phase = 'reap_pending' AND lifecycle.reap_reason = 'job_start_timeout')`

const releaseExpiredClusterAssignmentsSQL = `UPDATE scaler_assignments SET state = 'complete'
	WHERE assignment_id IN (
		SELECT assignment_id FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND ($2 = '' OR repository = $2)
		  AND phase = 'reap_pending' AND reap_reason = 'job_start_timeout' AND updated_at = $3
	) AND state <> 'complete'`

const confirmClusterRunnerReapedSQL = `DELETE FROM scaler_runner_lifecycle
	WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4
	  AND phase = 'reap_pending'`

const confirmClusterLibvirtRunnerReapedSQL = `DELETE FROM scaler_libvirt_runner_lifecycle
	WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4 AND reap_requested`

func (store *Store) RegisterRunner(ctx context.Context, runner scaler.RunnerLifecycle) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	commandTag, err := connection.Exec(ctx, registerClusterRunnerLifecycleSQL,
		runner.AssignmentID, runner.NodeID, runner.Provider, runner.RunnerName,
		runner.InstallationID, runner.GitHubScaleSetID, runner.GitHubRunnerID, runner.CreatedAt)
	if err != nil {
		return fmt.Errorf("register clustered runner lifecycle: %w", err)
	}
	if commandTag.RowsAffected() == 1 {
		return store.mirrorClusterLibvirtRegistration(ctx, connection, runner)
	}
	var matches bool
	if err = connection.QueryRow(ctx, verifyClusterRunnerLifecycleSQL,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName,
		runner.Provider, runner.RequestID, runner.AssignmentID).Scan(&matches); err != nil {
		return fmt.Errorf("verify clustered runner lifecycle: %w", err)
	}
	if !matches {
		return &AssignmentTransitionError{AssignmentID: AssignmentID(runner.AssignmentID), Expected: AssignmentStarting}
	}
	return nil
}

func (store *Store) mirrorClusterLibvirtRegistration(
	ctx context.Context,
	connection *databaseConnection,
	runner scaler.RunnerLifecycle,
) error {
	if runner.Provider != scaler.RunnerProviderLibvirt {
		return nil
	}
	_, err := connection.Exec(ctx, `INSERT INTO scaler_libvirt_runner_lifecycle
		(node_id, scale_set, repository, request_id, runner_name, reap_requested)
		VALUES($1, $2, $3, $4, $5, false)
		ON CONFLICT (node_id, scale_set, repository, runner_name) DO NOTHING`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("mirror clustered libvirt lifecycle: %w", err)
	}
	return nil
}

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

func (store *Store) MarkRunnerBusy(
	ctx context.Context,
	scaleSet, repository string,
	requestID int64,
	runnerName string,
	githubRunnerID int64,
	startedAt time.Time,
) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	commandTag, err := connection.Exec(ctx, `UPDATE scaler_runner_lifecycle
		SET phase = 'busy', github_runner_id = CASE WHEN $1 > 0 THEN $1 ELSE github_runner_id END, updated_at = $2
		WHERE scale_set = $3 AND repository = $4 AND phase <> 'reap_pending'
		  AND (($5 <> '' AND runner_name = $5) OR ($6 > 0 AND request_id = $6))`,
		githubRunnerID, startedAt, scaleSet, repository, runnerName, requestID)
	if err != nil {
		return fmt.Errorf("mark clustered runner busy: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (store *Store) RequestRunnerReap(
	ctx context.Context,
	scaleSet, repository string,
	requestID int64,
	runnerName, reason string,
	requestedAt time.Time,
) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	_, err = connection.Exec(ctx, `UPDATE scaler_runner_lifecycle
		SET phase = 'reap_pending', reap_reason = $1, updated_at = $2
		WHERE scale_set = $3 AND repository = $4
		  AND (($5 <> '' AND runner_name = $5) OR ($6 > 0 AND request_id = $6))`,
		reason, requestedAt, scaleSet, repository, runnerName, requestID)
	if err != nil {
		return fmt.Errorf("request clustered runner reap: %w", err)
	}
	if _, err = connection.Exec(ctx, `UPDATE scaler_libvirt_runner_lifecycle SET reap_requested = true
		WHERE scale_set = $1 AND repository = $2
		  AND (($3 <> '' AND runner_name = $3) OR ($4 > 0 AND request_id = $4))`,
		scaleSet, repository, runnerName, requestID); err != nil {
		return fmt.Errorf("mirror clustered libvirt runner reap: %w", err)
	}
	return nil
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
	if _, err = transaction.Exec(ctx, mirrorExpiredClusterRunnersSQL); err != nil {
		return 0, fmt.Errorf("mirror clustered expired libvirt runner reaps: %w", err)
	}
	if _, err = transaction.Exec(ctx, releaseExpiredClusterAssignmentsSQL, scaleSet, repository, requestedAt); err != nil {
		return 0, fmt.Errorf("release expired runner assignments: %w", err)
	}
	return changed, nil
}

func (*Store) RequestExcessWaitingRunnerReaps(
	context.Context, string, string, int, string, time.Time,
) (int, error) {
	return 0, nil
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
	commandTag, err := transaction.Exec(ctx, confirmClusterRunnerReapedSQL,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("confirm clustered runner reaped: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	if runner.Provider == scaler.RunnerProviderLibvirt {
		if _, err = transaction.Exec(ctx, confirmClusterLibvirtRunnerReapedSQL,
			runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName); err != nil {
			return fmt.Errorf("confirm mirrored clustered libvirt runner reap: %w", err)
		}
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit clustered runner reap confirmation: %w", err)
	}
	return nil
}

var _ scaler.RunnerLifecycleStore = (*Store)(nil)
