package scaler

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func normalizeSQLiteRunnerRegistration(runner RunnerLifecycle) RunnerLifecycle {
	if runner.CreatedAt.IsZero() {
		runner.CreatedAt = time.Now().UTC()
	}
	if runner.Phase == "" {
		runner.Phase = RunnerProvisioning
	}
	if runner.WaitingSince.IsZero() && (runner.Phase == RunnerWaitingForJob || runner.Phase == RunnerBusy) {
		runner.WaitingSince = runner.CreatedAt
	}
	if runner.ReapReason == "" && runner.Phase == RunnerReapPending {
		runner.ReapReason = "lifecycle_reap_requested"
	}
	return runner
}

func (store *SQLiteDemandStore) insertSQLiteRunnerRegistration(
	ctx context.Context,
	runner RunnerLifecycle,
) (sql.Result, error) {
	createdAt := runner.CreatedAt.UnixNano()
	waitingSince := unixNanoOrZero(runner.WaitingSince)
	var result sql.Result
	var err error
	if runner.AssignmentID > 0 {
		result, err = store.database.ExecContext(ctx, registerSQLiteRunnerLifecycleSQL,
			runner.NodeID, runner.Provider, runner.RunnerName, runner.InstallationID,
			runner.GitHubScaleSetID, runner.GitHubRunnerID, runner.Phase, createdAt,
			waitingSince, createdAt, runner.ReapReason, runner.AssignmentID)
	} else {
		result, err = store.database.ExecContext(ctx, registerUnassignedSQLiteRunnerLifecycleSQL,
			runner.NodeID, runner.ScaleSet, runner.Repository, runner.Provider, runner.RunnerName,
			runner.RequestID, runner.InstallationID, runner.GitHubScaleSetID, runner.GitHubRunnerID,
			runner.Phase, createdAt, waitingSince, createdAt, runner.ReapReason)
	}
	if err != nil {
		return nil, fmt.Errorf("insert runner registration: %w", err)
	}
	return result, nil
}
