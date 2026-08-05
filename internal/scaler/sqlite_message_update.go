package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ApplyMessageUpdate persists one complete GitHub message under the current
// standalone controller epoch.
func (store *SQLiteDemandStore) ApplyMessageUpdate(
	ctx context.Context,
	update MessageUpdate,
) (result MessageUpdateResult, resultErr error) {
	if !ValidMessageUpdate(update) {
		return result, ErrInvalidMessageUpdate
	}
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin scale-set message update: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "scale-set message update", &resultErr)
	if err = validateSQLiteMessageEpoch(ctx, transaction, update); err != nil {
		return result, err
	}
	if err = applySQLiteMessageEffects(ctx, transaction, update, &result); err != nil {
		return result, err
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return result, fmt.Errorf("commit scale-set message update: %w", err)
	}
	return result, nil
}

func validateSQLiteMessageEpoch(ctx context.Context, transaction *sql.Tx, update MessageUpdate) error {
	var current int
	err := transaction.QueryRowContext(ctx, `SELECT 1 FROM repository_controls
		WHERE app_id = ? AND controller_epoch = ?`, update.AppID, update.ControllerEpoch).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotLeader
	}
	if err != nil {
		return fmt.Errorf("validate message controller epoch: %w", err)
	}
	return nil
}

func applySQLiteMessageEffects(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
	result *MessageUpdateResult,
) error {
	newExactObligations, err := countNewSQLiteExactObligations(ctx, transaction, update)
	if err != nil {
		return err
	}
	if err = applySQLiteMessageLifecycleEffects(ctx, transaction, update); err != nil {
		return err
	}
	decision, err := reconcileSQLiteMessageStatistics(ctx, transaction, update)
	result.AssignedObligations = decision
	if err != nil {
		return err
	}
	return populateSQLiteMessageQueueSnapshot(ctx, transaction, update, newExactObligations, decision, result)
}

func applySQLiteMessageLifecycleEffects(ctx context.Context, transaction *sql.Tx, update MessageUpdate) error {
	removals := messageDemandRemovals(update)
	if err := applyDemandChanges(ctx, transaction, update.ScaleSet, update.Available, removals); err != nil {
		return err
	}
	if err := applySQLiteMessageStarts(ctx, transaction, update); err != nil {
		return err
	}
	return applySQLiteMessageCompletions(ctx, transaction, update)
}

func applySQLiteMessageStarts(ctx context.Context, transaction *sql.Tx, update MessageUpdate) error {
	for _, started := range update.Started {
		if err := markSQLiteMessageRunnerBusy(ctx, transaction, update, started); err != nil {
			if !errors.Is(err, ErrRunnerIdentityNotFound) {
				return err
			}
		}
	}
	return nil
}

func applySQLiteMessageCompletions(ctx context.Context, transaction *sql.Tx, update MessageUpdate) error {
	for _, completed := range update.Completed {
		if err := completeSQLiteRunnerJob(ctx, transaction, JobCompletion{
			ScaleSet: update.ScaleSet, Repository: update.Repository,
			RunnerIdentity: completed.RunnerIdentity,
		}, update.ObservedAt); err != nil {
			if !errors.Is(err, ErrRunnerIdentityNotFound) {
				return err
			}
		}
	}
	return nil
}

func reconcileSQLiteMessageStatistics(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
) (AssignedObligationDecision, error) {
	activeCapacity, err := sqliteMessageActiveCapacity(ctx, transaction, update.ScaleSet, update.Repository)
	if err != nil {
		return AssignedObligationDecision{}, err
	}
	decision, err := reconcileAssignedCapacityObservation(ctx, transaction, AssignedCapacityObservation{
		ScaleSet: update.ScaleSet, Repository: update.Repository, ControllerEpoch: update.ControllerEpoch,
		AssignedJobs: update.AssignedJobs, RunningJobs: update.RunningJobs,
		RegisteredRunners: update.RegisteredRunners, BusyRunners: update.BusyRunners,
		ObservedAt: update.ObservedAt,
	}, activeCapacity)
	return decision, err
}

func populateSQLiteMessageQueueSnapshot(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
	newExactObligations int,
	decision AssignedObligationDecision,
	result *MessageUpdateResult,
) error {
	queueSnapshot, err := sqliteCapacityObligationQueueSnapshot(ctx, transaction, update.ScaleSet, update.ObservedAt)
	if err != nil {
		return err
	}
	queueSnapshot.NewlyQueued = newExactObligations + decision.Create
	queueSnapshot.Canceled = decision.Cancel
	result.QueueSnapshot = queueSnapshot
	return nil
}

func countNewSQLiteExactObligations(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
) (int, error) {
	seen := make(map[int64]struct{}, len(update.Available))
	newObligations := 0
	for _, demand := range update.Available {
		if _, duplicate := seen[demand.RequestID]; duplicate {
			continue
		}
		seen[demand.RequestID] = struct{}{}
		var existing int
		err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM demand
			WHERE scale_set = ? AND repository = ? AND request_id = ? AND source = 'job_available'`,
			update.ScaleSet, update.Repository, demand.RequestID).Scan(&existing)
		if err != nil {
			return 0, fmt.Errorf("inspect existing exact capacity obligation: %w", err)
		}
		if existing == 0 {
			newObligations++
		}
	}
	return newObligations, nil
}

func sqliteCapacityObligationQueueSnapshot(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet string,
	runnableAt time.Time,
) (CapacityObligationQueueSnapshot, error) {
	var snapshot CapacityObligationQueueSnapshot
	var oldestRunnableNanoseconds sql.NullInt64
	err := transaction.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN source = 'job_available' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN source = 'assigned_count' THEN 1 ELSE 0 END), 0),
		MIN(CASE WHEN blocked_reason IS NULL AND next_attempt_at <= ? AND (
			source = 'job_available' OR NOT EXISTS (
				SELECT 1 FROM runner_lifecycle lifecycle WHERE lifecycle.assignment_id = demand.demand_id
			)
		) THEN queue_time END)
		FROM demand WHERE scale_set = ?`, runnableAt.UnixNano(), scaleSet).
		Scan(&snapshot.JobAvailable, &snapshot.AssignedCount, &oldestRunnableNanoseconds)
	if err != nil {
		return snapshot, fmt.Errorf("load capacity obligation queue snapshot: %w", err)
	}
	if oldestRunnableNanoseconds.Valid {
		oldestRunnableAt := time.Unix(0, oldestRunnableNanoseconds.Int64).UTC()
		snapshot.OldestRunnableAt = &oldestRunnableAt
	}
	return snapshot, nil
}

func sqliteMessageActiveCapacity(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet string,
	repository string,
) (int, error) {
	var active int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'`, scaleSet, repository).Scan(&active); err != nil {
		return 0, fmt.Errorf("count post-message runner capacity: %w", err)
	}
	return active, nil
}

func messageDemandRemovals(update MessageUpdate) []DemandRemoval {
	removals := make([]DemandRemoval, 0, len(update.Started)+len(update.Completed))
	for _, started := range update.Started {
		if started.RequestID > 0 {
			removals = append(removals, DemandRemoval{Repository: update.Repository, RequestID: started.RequestID})
		}
	}
	for _, completed := range update.Completed {
		if completed.RequestID > 0 {
			removals = append(removals, DemandRemoval{Repository: update.Repository, RequestID: completed.RequestID})
		}
	}
	return removals
}

func markSQLiteMessageRunnerBusy(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
	started RunnerMessageIdentity,
) error {
	identity := started.RunnerIdentity
	resolveErr := resolveSQLiteRunnerIdentity(
		ctx, transaction, update.ScaleSet, update.Repository, activeSQLiteRunner, identity,
	)
	if resolveErr == nil {
		if err := updateSQLiteRunnerBusy(ctx, transaction, update.ScaleSet, update.Repository,
			identity, started.GitHubRunnerID, update.ObservedAt); err != nil {
			return err
		}
		return nil
	}
	if identity.isPair() && (errors.Is(resolveErr, ErrRunnerIdentityMismatch) ||
		errors.Is(resolveErr, ErrRunnerIdentityNotFound)) {
		return bindSQLiteRunnerIdentity(ctx, transaction, update.ScaleSet, update.Repository,
			identity, started.GitHubRunnerID, update.ObservedAt)
	}
	return resolveErr
}
