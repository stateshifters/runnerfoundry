package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const registerSQLiteRunnerLifecycleSQL = `INSERT INTO runner_lifecycle(
	node_id, scale_set, repository, provider, runner_name, request_id, assignment_id, installation_id, github_scale_set_id,
	github_runner_id, phase, created_at, waiting_since, updated_at, reap_reason)
	SELECT ?, demand.scale_set, demand.repository, ?, ?, COALESCE(demand.request_id, 0),
		demand.demand_id, ?, ?, ?, ?, ?, ?, ?, ?
	FROM demand WHERE demand.demand_id = ?
	ON CONFLICT(scale_set, repository, runner_name) DO NOTHING`

const registerUnassignedSQLiteRunnerLifecycleSQL = `INSERT INTO runner_lifecycle(
	node_id, scale_set, repository, provider, runner_name, request_id, assignment_id, installation_id, github_scale_set_id,
	github_runner_id, phase, created_at, waiting_since, updated_at, reap_reason)
	VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(scale_set, repository, runner_name) DO NOTHING`

const inspectSQLiteRunnerLifecycleSQL = `SELECT provider, request_id, COALESCE(assignment_id, 0), phase
	FROM runner_lifecycle WHERE scale_set = ? AND repository = ? AND runner_name = ?`

const pendingSQLiteRunnerLifecyclesSQL = `SELECT node_id, scale_set, repository, provider, runner_name,
	request_id, assignment_id, installation_id, github_scale_set_id, github_runner_id, phase,
	created_at, waiting_since, updated_at, reap_reason
	FROM (
		SELECT lifecycle.node_id,
			COALESCE(demand.scale_set, lifecycle.scale_set) AS scale_set,
			COALESCE(demand.repository, lifecycle.repository) AS repository,
			lifecycle.provider, lifecycle.runner_name,
			COALESCE(demand.request_id, lifecycle.request_id) AS request_id,
			COALESCE(lifecycle.assignment_id, 0) AS assignment_id,
			lifecycle.installation_id, lifecycle.github_scale_set_id, lifecycle.github_runner_id,
			lifecycle.phase, lifecycle.created_at, lifecycle.waiting_since, lifecycle.updated_at,
			lifecycle.reap_reason
		FROM runner_lifecycle lifecycle LEFT JOIN demand ON demand.demand_id = lifecycle.assignment_id
	) authoritative_lifecycle
	WHERE scale_set = ? AND (? = '' OR repository = ?) AND phase = 'reap_pending'
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

// Identity returns the persisted request/name pair used for exact mutations.
func (runner RunnerLifecycle) Identity() RunnerIdentity {
	return RunnerIdentity{RequestID: runner.RequestID, RunnerName: runner.RunnerName}
}

// RunnerLifecycleTransitions owns validated selection for single-runner state changes.
// ID-only and name-only identities select by that value. A pair requires both
// values on the same eligible row. BindRunnerIdentity is the only exception:
// it may atomically attach a positive request ID to the uniquely name-matched
// identity-free row, and an already-bound pair succeeds idempotently.
//
// Implementations return ErrInvalidRunnerIdentity for unusable input,
// ErrRunnerIdentityNotFound when no eligible row exists,
// ErrRunnerIdentityMismatch when either field of a pair exists on another row,
// and ErrRunnerIdentityInvariant when an identity observes multiple rows. No
// mutation may commit for mismatch or invariant outcomes. Existing exact
// transitions are idempotent; callers handling at-least-once messages may treat
// not-found as an already-applied event.
type RunnerLifecycleTransitions interface {
	RegisterRunner(ctx context.Context, runner RunnerLifecycle) error
	ConfirmRunnerWaiting(ctx context.Context, runner RunnerLifecycle, confirmedAt time.Time) error
	MarkRunnerBusy(ctx context.Context, scaleSet, repository string, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time) error
	BindRunnerIdentity(ctx context.Context, scaleSet, repository string, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time) error
	RequestRunnerReap(ctx context.Context, scaleSet, repository string, identity RunnerIdentity, reason string, requestedAt time.Time) error
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
	// CompleteRunnerLifecycleJob atomically requests cleanup and removes demand
	// for exactly one runner selected by completion.RunnerIdentity.
	CompleteRunnerLifecycleJob(ctx context.Context, completion JobCompletion, completedAt time.Time) error
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
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin mark runner busy: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "mark runner busy", &resultErr)
	if err = resolveSQLiteRunnerIdentity(ctx, transaction, scaleSet, repository, activeSQLiteRunner, identity); err != nil {
		return err
	}
	if err = updateSQLiteRunnerBusy(ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt); err != nil {
		return err
	}
	if err = reconcileSQLiteMirrorForIdentity(ctx, transaction, scaleSet, repository, identity); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit mark runner busy: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) RequestRunnerReap(
	ctx context.Context,
	scaleSet, repository string,
	identity RunnerIdentity,
	reason string,
	requestedAt time.Time,
) error {
	return store.runSQLiteTransaction(ctx, "request runner reap", func(transaction *sql.Tx) error {
		return requestSQLiteRunnerReap(ctx, transaction, scaleSet, repository, identity, reason, requestedAt)
	})
}

func requestSQLiteRunnerReap(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
	reason string,
	requestedAt time.Time,
) error {
	var err error
	if err = resolveSQLiteRunnerIdentity(ctx, transaction, scaleSet, repository, anySQLiteRunner, identity); err != nil {
		return err
	}
	if err = updateSQLiteRunnerReapRequest(
		ctx, transaction, scaleSet, repository, identity, reason, requestedAt,
	); err != nil {
		return err
	}
	return mirrorSQLiteRunnerReapRequest(ctx, transaction, scaleSet, repository, identity)
}

func updateSQLiteRunnerReapRequest(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
	reason string,
	requestedAt time.Time,
) error {
	result, err := executeSQLiteIdentityStatement(ctx, transaction, identity,
		[]any{reason, requestedAt.UnixNano(), scaleSet, repository},
		`UPDATE runner_lifecycle SET phase = 'reap_pending', reap_reason = ?, updated_at = ?
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		`UPDATE runner_lifecycle SET phase = 'reap_pending', reap_reason = ?, updated_at = ?
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		`UPDATE runner_lifecycle SET phase = 'reap_pending', reap_reason = ?, updated_at = ?
		WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name = ?`,
		"request runner reap")
	if err != nil {
		return err
	}
	if err = requireSingleSQLiteIdentityChange(result, identity, "count requested runner reap"); err != nil {
		return err
	}
	return nil
}

func mirrorSQLiteRunnerReapRequest(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
) error {
	mirrorResult, err := executeSQLiteIdentityStatement(ctx, transaction, identity,
		[]any{scaleSet, repository},
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name = ?`,
		"mirror requested libvirt runner reap")
	if err != nil {
		return err
	}
	changed, err := mirrorResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("count mirrored runner reap: %w", err)
	}
	if changed > 1 {
		return &RunnerIdentityInvariantError{Identity: identity, Matches: changed}
	}
	return reconcileSQLiteMirrorForIdentity(ctx, transaction, scaleSet, repository, identity)
}

func (store *SQLiteDemandStore) RequestExpiredRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
	deadline, requestedAt time.Time,
) (int, error) {
	changed := 0
	err := store.runSQLiteTransaction(ctx, "expired runner reap request", func(transaction *sql.Tx) error {
		var transitionErr error
		changed, transitionErr = requestExpiredSQLiteRunnerReaps(
			ctx, transaction, scaleSet, repository, deadline, requestedAt,
		)
		return transitionErr
	})
	return changed, err
}

func requestExpiredSQLiteRunnerReaps(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	deadline, requestedAt time.Time,
) (int, error) {
	result, err := transaction.ExecContext(ctx, `UPDATE runner_lifecycle
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
	return changed, reconcileSQLiteBulkReapMirrors(ctx, transaction, scaleSet, repository, "job_start_timeout")
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
	changed := 0
	err := store.runSQLiteTransaction(ctx, "excess runner reap request", func(transaction *sql.Tx) error {
		var transitionErr error
		changed, transitionErr = requestExcessSQLiteRunnerReaps(
			ctx, transaction, scaleSet, repository, retain, reason, requestedAt,
		)
		return transitionErr
	})
	return changed, err
}

func requestExcessSQLiteRunnerReaps(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	retain int,
	reason string,
	requestedAt time.Time,
) (int, error) {
	changed, err := requestExcessSQLiteGenericRunnerReaps(
		ctx, transaction, scaleSet, repository, retain, reason, requestedAt,
	)
	if err != nil {
		return 0, err
	}
	return changed, reconcileSQLiteBulkReapMirrors(ctx, transaction, scaleSet, repository, reason)
}

func requestExcessSQLiteGenericRunnerReaps(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	retain int,
	reason string,
	requestedAt time.Time,
) (int, error) {
	result, err := transaction.ExecContext(ctx, `UPDATE runner_lifecycle
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
	return lifecycleRowsAffected(result, "count excess waiting runner reaps")
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
		FROM (
			SELECT lifecycle.node_id,
				COALESCE(demand.scale_set, lifecycle.scale_set) AS scale_set,
				COALESCE(demand.repository, lifecycle.repository) AS repository,
				lifecycle.provider, lifecycle.runner_name,
				COALESCE(demand.request_id, lifecycle.request_id) AS request_id,
				COALESCE(lifecycle.assignment_id, 0) AS assignment_id,
				lifecycle.installation_id, lifecycle.github_scale_set_id, lifecycle.github_runner_id,
				lifecycle.phase, lifecycle.created_at, lifecycle.waiting_since, lifecycle.updated_at,
				lifecycle.reap_reason
			FROM runner_lifecycle lifecycle LEFT JOIN demand ON demand.demand_id = lifecycle.assignment_id
		) authoritative_lifecycle
		WHERE scale_set = ? AND (? = '' OR repository = ?)
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

func (store *SQLiteDemandStore) ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) error {
	return store.runSQLiteTransaction(ctx, "runner reap confirmation", func(transaction *sql.Tx) error {
		return confirmSQLiteRunnerReaped(ctx, transaction, runner)
	})
}

func confirmSQLiteRunnerReaped(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) error {
	identity := runner.Identity()
	if err := resolveSQLiteRunnerIdentity(
		ctx, transaction, runner.ScaleSet, runner.Repository, pendingSQLiteRunner, identity,
	); err != nil {
		return err
	}
	authoritative, err := authoritativeSQLiteTerminalRunner(ctx, transaction, runner)
	if err != nil {
		return err
	}
	if err = deleteSQLiteGenericRunner(ctx, transaction, runner); err != nil {
		return err
	}
	if authoritative.Provider != RunnerProviderLibvirt {
		return nil
	}
	return deleteSQLiteMirror(ctx, transaction, authoritative)
}

func authoritativeSQLiteTerminalRunner(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
) (RunnerLifecycle, error) {
	authoritative, err := loadSQLiteAuthoritativeRunner(
		ctx, transaction, runner.ScaleSet, runner.Repository, runner.Identity(),
	)
	if err != nil {
		return RunnerLifecycle{}, err
	}
	if err = validateSQLiteCompatibleMirror(ctx, transaction, authoritative); err != nil {
		return RunnerLifecycle{}, err
	}
	return authoritative, nil
}

func deleteSQLiteGenericRunner(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) error {
	identity := runner.Identity()
	result, err := executeSQLiteIdentityStatement(ctx, transaction, identity,
		[]any{runner.ScaleSet, runner.Repository},
		`DELETE FROM runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND phase = 'reap_pending' AND request_id = ?`,
		`DELETE FROM runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND phase = 'reap_pending' AND runner_name = ?`,
		`DELETE FROM runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND phase = 'reap_pending' AND request_id = ? AND runner_name = ?`,
		"confirm runner reaped")
	if err != nil {
		return err
	}
	if err = requireSingleSQLiteIdentityChange(result, identity, "count confirmed runner reap"); err != nil {
		return err
	}
	return nil
}

func confirmSQLiteMirroredRunnerReaped(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
) error {
	identity := runner.Identity()
	result, err := executeSQLiteIdentityStatement(ctx, transaction, identity,
		[]any{runner.ScaleSet, runner.Repository},
		`DELETE FROM libvirt_runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND reap_requested = 1 AND request_id = ?`,
		`DELETE FROM libvirt_runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND reap_requested = 1 AND runner_name = ?`,
		`DELETE FROM libvirt_runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND reap_requested = 1 AND request_id = ? AND runner_name = ?`,
		"confirm mirrored libvirt runner reap")
	if err != nil {
		return err
	}
	return requireAtMostOneSQLiteIdentityChange(result, identity,
		"count confirmed mirrored libvirt runner reap")
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
