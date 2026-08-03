package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const completeSQLiteRunnerLifecycleSQL = `UPDATE runner_lifecycle
	SET phase = 'reap_pending', reap_reason = 'job_completed', updated_at = ?
	WHERE scale_set = ? AND repository = ?
	AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))`

const mirrorCompletedSQLiteLibvirtSQL = `UPDATE libvirt_runner_lifecycle SET reap_requested = 1
	WHERE scale_set = ? AND repository = ?
	AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))`

const registerSQLiteRunnerLifecycleSQL = `INSERT INTO runner_lifecycle(
	node_id, scale_set, repository, provider, runner_name, request_id, assignment_id, installation_id, github_scale_set_id,
	github_runner_id, phase, created_at, waiting_since, updated_at, reap_reason)
	VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(scale_set, repository, runner_name) DO NOTHING`

const inspectSQLiteRunnerLifecycleSQL = `SELECT provider, request_id, assignment_id, phase
	FROM runner_lifecycle WHERE scale_set = ? AND repository = ? AND runner_name = ?`

const pendingSQLiteRunnerLifecyclesSQL = `SELECT node_id, scale_set, repository, provider, runner_name,
	request_id, assignment_id, installation_id, github_scale_set_id, github_runner_id, phase, created_at, waiting_since, updated_at, reap_reason
	FROM runner_lifecycle WHERE scale_set = ? AND (? = '' OR repository = ?) AND phase = 'reap_pending'
	ORDER BY repository, runner_name`

type RunnerProvider string

const (
	RunnerProviderDocker  RunnerProvider = "docker"
	RunnerProviderLibvirt RunnerProvider = "libvirt"
)

type RunnerLifecyclePhase string

const (
	RunnerProvisioning  RunnerLifecyclePhase = "provisioning"
	RunnerWaitingForJob RunnerLifecyclePhase = "waiting_for_job"
	RunnerBusy          RunnerLifecyclePhase = "busy"
	RunnerReapPending   RunnerLifecyclePhase = "reap_pending"
)

type RunnerLifecycle struct {
	NodeID           string
	ScaleSet         string
	Repository       string
	Provider         RunnerProvider
	RunnerName       string
	RequestID        int64
	AssignmentID     int64
	InstallationID   int64
	GitHubScaleSetID int
	GitHubRunnerID   int64
	Phase            RunnerLifecyclePhase
	CreatedAt        time.Time
	WaitingSince     time.Time
	UpdatedAt        time.Time
	ReapReason       string
}

type RunnerLifecycleTransitions interface {
	RegisterRunner(ctx context.Context, runner RunnerLifecycle) error
	ConfirmRunnerWaiting(ctx context.Context, runner RunnerLifecycle, confirmedAt time.Time) error
	MarkRunnerBusy(ctx context.Context, scaleSet, repository string, requestID int64, runnerName string, githubRunnerID int64, startedAt time.Time) error
	RequestRunnerReap(ctx context.Context, scaleSet, repository string, requestID int64, runnerName, reason string, requestedAt time.Time) error
}

type RunnerLifecycleCleanupStore interface {
	RequestExpiredRunnerReaps(ctx context.Context, scaleSet, repository string, deadline, requestedAt time.Time) (int, error)
	RequestExcessWaitingRunnerReaps(ctx context.Context, scaleSet, repository string, retain int, reason string, requestedAt time.Time) (int, error)
	PendingRunnerReaps(ctx context.Context, scaleSet, repository string) ([]RunnerLifecycle, error)
	ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) error
}

type RunnerLifecycleStore interface {
	RunnerLifecycleTransitions
	RunnerLifecycleCleanupStore
}

type RunnerLifecycleInventory interface {
	RunnerLifecycles(ctx context.Context, scaleSet, repository string) ([]RunnerLifecycle, error)
}

type RunnerJobCompletionStore interface {
	CompleteRunnerLifecycleJob(ctx context.Context, completion JobCompletion, completedAt time.Time) error
}

func (store *SQLiteDemandStore) CompleteRunnerLifecycleJob(
	ctx context.Context,
	completion JobCompletion,
	completedAt time.Time,
) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin runner job completion: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "runner job completion", &resultErr)
	if err = completeSQLiteRunnerJob(ctx, transaction, completion, completedAt); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit runner job completion: %w", err)
	}
	return nil
}

func completeSQLiteRunnerJob(ctx context.Context, transaction *sql.Tx, completion JobCompletion, completedAt time.Time) error {
	identity := []any{completion.ScaleSet, completion.Repository, completion.RunnerName, completion.RunnerName,
		completion.RequestID, completion.RequestID}
	if _, err := transaction.ExecContext(ctx, completeSQLiteRunnerLifecycleSQL, append([]any{completedAt.UnixNano()}, identity...)...); err != nil {
		return fmt.Errorf("request completed runner reap: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, mirrorCompletedSQLiteLibvirtSQL, identity...); err != nil {
		return fmt.Errorf("mirror completed libvirt runner reap: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM demand
		WHERE source = 'assigned_count' AND demand_id IN (
			SELECT assignment_id FROM runner_lifecycle
			WHERE scale_set = ? AND repository = ?
			AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))
		)`, identity...); err != nil {
		return fmt.Errorf("complete assigned capacity obligation: %w", err)
	}
	if completion.RequestID <= 0 {
		return nil
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM demand
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return fmt.Errorf("remove completed demand: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) RegisterRunner(ctx context.Context, runner RunnerLifecycle) error {
	createdAt := runner.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	phase := runner.Phase
	if phase == "" {
		phase = RunnerProvisioning
	}
	result, err := store.database.ExecContext(ctx, registerSQLiteRunnerLifecycleSQL,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.Provider, runner.RunnerName,
		runner.RequestID, runner.AssignmentID, runner.InstallationID, runner.GitHubScaleSetID, runner.GitHubRunnerID, phase,
		createdAt.UnixNano(), unixNanoOrZero(runner.WaitingSince), createdAt.UnixNano(), runner.ReapReason)
	if err != nil {
		return fmt.Errorf("register runner lifecycle: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count registered runner lifecycle: %w", err)
	}
	if inserted == 1 {
		return store.mirrorLibvirtRegistration(ctx, runner)
	}
	return store.verifyMatchingRunnerRegistration(ctx, runner)
}

func (store *SQLiteDemandStore) verifyMatchingRunnerRegistration(ctx context.Context, runner RunnerLifecycle) error {
	var requestID, assignmentID int64
	var provider string
	var existingPhase string
	err := store.database.QueryRowContext(ctx, inspectSQLiteRunnerLifecycleSQL,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&provider, &requestID, &assignmentID, &existingPhase)
	if err != nil {
		return fmt.Errorf("inspect registered runner lifecycle: %w", err)
	}
	if provider != string(runner.Provider) || requestID != runner.RequestID || assignmentID != runner.AssignmentID ||
		existingPhase == string(RunnerReapPending) {
		return errors.New("runner identity remains reserved by another lifecycle")
	}
	return nil
}

func (store *SQLiteDemandStore) mirrorLibvirtRegistration(ctx context.Context, runner RunnerLifecycle) error {
	if runner.Provider != RunnerProviderLibvirt {
		return nil
	}
	_, err := store.database.ExecContext(ctx, `INSERT INTO libvirt_runner_lifecycle
		(scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(scale_set, repository, runner_name) DO NOTHING`,
		runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName, runner.Phase == RunnerReapPending)
	if err != nil {
		return fmt.Errorf("mirror libvirt runner lifecycle: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) ConfirmRunnerWaiting(ctx context.Context, runner RunnerLifecycle, confirmedAt time.Time) error {
	result, err := store.database.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'waiting_for_job', github_runner_id = ?, waiting_since = ?, updated_at = ?
		WHERE scale_set = ? AND repository = ? AND runner_name = ? AND phase = 'provisioning'`,
		runner.GitHubRunnerID, confirmedAt.UnixNano(), confirmedAt.UnixNano(), runner.ScaleSet, runner.Repository, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("confirm runner waiting: %w", err)
	}
	return requireLifecycleChange(result, "confirm runner waiting")
}

func (store *SQLiteDemandStore) MarkRunnerBusy(
	ctx context.Context,
	scaleSet, repository string,
	requestID int64,
	runnerName string,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	result, err := store.database.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'busy', request_id = CASE WHEN request_id = 0 AND ? > 0 THEN ? ELSE request_id END,
		github_runner_id = CASE WHEN ? > 0 THEN ? ELSE github_runner_id END, updated_at = ?
		WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'
		AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))`,
		requestID, requestID, githubRunnerID, githubRunnerID, startedAt.UnixNano(), scaleSet, repository,
		runnerName, runnerName, requestID, requestID)
	if err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return requireLifecycleChange(result, "mark runner busy")
}

func (store *SQLiteDemandStore) RequestRunnerReap(
	ctx context.Context,
	scaleSet, repository string,
	requestID int64,
	runnerName, reason string,
	requestedAt time.Time,
) error {
	result, err := store.database.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'reap_pending', reap_reason = ?, updated_at = ?
		WHERE scale_set = ? AND repository = ?
		AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))`,
		reason, requestedAt.UnixNano(), scaleSet, repository, runnerName, runnerName, requestID, requestID)
	if err != nil {
		return fmt.Errorf("request runner reap: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count requested runner reaps: %w", err)
	}
	if changed == 0 {
		return nil
	}
	if _, err = store.database.ExecContext(ctx, `UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ?
		AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))`,
		scaleSet, repository, runnerName, runnerName, requestID, requestID); err != nil {
		return fmt.Errorf("mirror requested libvirt runner reap: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) RequestExpiredRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
	deadline, requestedAt time.Time,
) (int, error) {
	result, err := store.database.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'reap_pending', reap_reason = 'job_start_timeout', updated_at = ?
		WHERE scale_set = ? AND (? = '' OR repository = ?) AND phase = 'waiting_for_job'
		AND waiting_since > 0 AND waiting_since <= ?`, requestedAt.UnixNano(), scaleSet, repository, repository, deadline.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("request expired runner reaps: %w", err)
	}
	changed, err := lifecycleRowsAffected(result, "count expired runner reaps")
	if err != nil {
		return 0, err
	}
	if _, err = store.database.ExecContext(ctx, `UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
			WHERE lifecycle.scale_set = libvirt_runner_lifecycle.scale_set
			  AND lifecycle.repository = libvirt_runner_lifecycle.repository
			  AND lifecycle.runner_name = libvirt_runner_lifecycle.runner_name
			  AND lifecycle.phase = 'reap_pending' AND lifecycle.reap_reason = 'job_start_timeout')`); err != nil {
		return 0, fmt.Errorf("mirror expired libvirt runner reaps: %w", err)
	}
	return changed, nil
}

func (store *SQLiteDemandStore) RequestExcessWaitingRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
	retain int,
	reason string,
	requestedAt time.Time,
) (int, error) {
	if retain < 0 {
		retain = 0
	}
	result, err := store.database.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'reap_pending', reap_reason = ?, updated_at = ?
		WHERE rowid IN (
			SELECT rowid FROM runner_lifecycle
			WHERE scale_set = ? AND repository = ? AND request_id = 0 AND phase = 'waiting_for_job'
			ORDER BY waiting_since DESC, runner_name DESC LIMIT -1 OFFSET MAX(0, ? - (
				SELECT COUNT(*) FROM runner_lifecycle busy
				WHERE busy.scale_set = ? AND busy.repository = ? AND busy.phase = 'busy'
			))
		)`, reason, requestedAt.UnixNano(), scaleSet, repository, retain, scaleSet, repository)
	if err != nil {
		return 0, fmt.Errorf("request excess waiting runner reaps: %w", err)
	}
	changed, err := lifecycleRowsAffected(result, "count excess waiting runner reaps")
	if err != nil {
		return 0, err
	}
	if _, err = store.database.ExecContext(ctx, `UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
			WHERE lifecycle.scale_set = libvirt_runner_lifecycle.scale_set
			  AND lifecycle.repository = libvirt_runner_lifecycle.repository
			  AND lifecycle.runner_name = libvirt_runner_lifecycle.runner_name
			  AND lifecycle.phase = 'reap_pending' AND lifecycle.reap_reason = ?)`, reason); err != nil {
		return 0, fmt.Errorf("mirror excess libvirt runner reaps: %w", err)
	}
	return changed, nil
}

func (store *SQLiteDemandStore) PendingRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
) (runners []RunnerLifecycle, resultErr error) {
	rows, err := store.database.QueryContext(ctx, pendingSQLiteRunnerLifecyclesSQL, scaleSet, repository, repository)
	if err != nil {
		return nil, fmt.Errorf("load pending runner reaps: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		runner, scanErr := scanSQLiteRunnerLifecycle(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan pending runner reap: %w", scanErr)
		}
		runners = append(runners, runner)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending runner reaps: %w", err)
	}
	return runners, nil
}

func (store *SQLiteDemandStore) RunnerLifecycles(
	ctx context.Context,
	scaleSet, repository string,
) (runners []RunnerLifecycle, resultErr error) {
	rows, err := store.database.QueryContext(ctx, `SELECT node_id, scale_set, repository, provider, runner_name,
		request_id, assignment_id, installation_id, github_scale_set_id, github_runner_id, phase,
		created_at, waiting_since, updated_at, reap_reason
		FROM runner_lifecycle WHERE scale_set = ? AND (? = '' OR repository = ?)
		ORDER BY repository, runner_name`, scaleSet, repository, repository)
	if err != nil {
		return nil, fmt.Errorf("load runner lifecycles: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		runner, scanErr := scanSQLiteRunnerLifecycle(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		runners = append(runners, runner)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runner lifecycles: %w", err)
	}
	return runners, nil
}

type sqliteRowScanner interface {
	Scan(dest ...any) error
}

func scanSQLiteRunnerLifecycle(row sqliteRowScanner) (RunnerLifecycle, error) {
	var runner RunnerLifecycle
	var provider, phase string
	var createdAt, waitingSince, updatedAt int64
	if err := row.Scan(&runner.NodeID, &runner.ScaleSet, &runner.Repository, &provider, &runner.RunnerName,
		&runner.RequestID, &runner.AssignmentID, &runner.InstallationID, &runner.GitHubScaleSetID,
		&runner.GitHubRunnerID, &phase, &createdAt, &waitingSince, &updatedAt, &runner.ReapReason); err != nil {
		return RunnerLifecycle{}, fmt.Errorf("scan runner lifecycle: %w", err)
	}
	runner.Provider = RunnerProvider(provider)
	runner.Phase = RunnerLifecyclePhase(phase)
	runner.CreatedAt = timeFromUnixNano(createdAt)
	runner.WaitingSince = timeFromUnixNano(waitingSince)
	runner.UpdatedAt = timeFromUnixNano(updatedAt)
	return runner, nil
}

func (store *SQLiteDemandStore) ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin runner reap confirmation: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "runner reap confirmation", &resultErr)
	result, err := transaction.ExecContext(ctx, `DELETE FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ? AND phase = 'reap_pending'`,
		runner.ScaleSet, runner.Repository, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("confirm runner reaped: %w", err)
	}
	if err = requireLifecycleChange(result, "confirm runner reaped"); err != nil {
		return err
	}
	if runner.Provider == RunnerProviderLibvirt {
		if _, err = transaction.ExecContext(ctx, `DELETE FROM libvirt_runner_lifecycle
			WHERE scale_set = ? AND repository = ? AND runner_name = ? AND reap_requested = 1`,
			runner.ScaleSet, runner.Repository, runner.RunnerName); err != nil {
			return fmt.Errorf("confirm mirrored libvirt runner reap: %w", err)
		}
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit runner reap confirmation: %w", err)
	}
	return nil
}

func requireLifecycleChange(result sql.Result, operation string) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if changed == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func lifecycleRowsAffected(result sql.Result, operation string) (int, error) {
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}
	return int(changed), nil
}

func unixNanoOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func timeFromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}
