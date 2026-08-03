package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type scaleSetHandler struct {
	Reconciler scaler.Reconciler
}

type coordinatedScaleSetHandler struct {
	Coordinator *scaler.Coordinator
	Repository  string
	Logger      *slog.Logger
	ScaleSet    string
	ReapLibvirt bool // retained for source compatibility; lifecycle cleanup is provider-independent
}

func (handler coordinatedScaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	logScaleSetSessionStatistics(
		ctx, handler.Logger, handler.Repository, handler.ScaleSet, "message", message.Statistics,
	)
	if err := handler.applyMessageLifecycle(ctx, message); err != nil {
		return err
	}
	return handler.reconcileAssignedCapacity(ctx, message.Statistics.TotalAssignedJobs)
}

func (handler coordinatedScaleSetHandler) applyMessageLifecycle(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if err := handler.markStartedRunnersBusy(ctx, message.JobStartedMessages); err != nil {
		return err
	}
	observedDemand, removedDemand, err := handler.collectDemand(message)
	if err != nil {
		return err
	}
	if applyErr := handler.Coordinator.Apply(ctx, observedDemand, removedDemand); applyErr != nil {
		return fmt.Errorf("persist GitHub job demand: %w", applyErr)
	}
	if err = handler.completeJobs(ctx, message.JobCompletedMessages); err != nil {
		return err
	}
	return nil
}

func (handler coordinatedScaleSetHandler) reconcileAssignedCapacity(ctx context.Context, assignedJobs int) error {
	reconciliation, err := handler.Coordinator.ReconcileAssigned(ctx, handler.Repository, assignedJobs)
	if err != nil {
		return fmt.Errorf("schedule shared runner capacity: %w", err)
	}
	effectiveLogger(handler.Logger).Info(
		"assigned capacity reconciled",
		slog.String("action", "assigned_recovery"),
		slog.String("phase", "completed"),
		slog.String("repository", handler.Repository),
		slog.String("scaleSet", handler.ScaleSet),
		slog.Int("assignedJobs", reconciliation.AssignedJobs),
		slog.Int("runningRunners", reconciliation.RunningRunners),
		slog.Int("startedRunners", reconciliation.StartedRunners),
	)
	return nil
}

func logScaleSetSessionStatistics(
	ctx context.Context,
	logger *slog.Logger,
	repository string,
	scaleSet string,
	source string,
	statistics *upstream.RunnerScaleSetStatistic,
) {
	logAction(ctx, logger, slog.LevelInfo, "session_statistics", "observed",
		slog.String("source", source), slog.String("repository", repository), slog.String("scaleSet", scaleSet),
		slog.Int("assignedJobs", statistics.TotalAssignedJobs), slog.Int("availableJobs", statistics.TotalAvailableJobs),
		slog.Int("acquiredJobs", statistics.TotalAcquiredJobs), slog.Int("runningJobs", statistics.TotalRunningJobs),
		slog.Int("registeredRunners", statistics.TotalRegisteredRunners), slog.Int("busyRunners", statistics.TotalBusyRunners),
		slog.Int("idleRunners", statistics.TotalIdleRunners))
}

func (handler coordinatedScaleSetHandler) collectDemand(message *upstream.RunnerScaleSetMessage) ([]scaler.JobDemand, []scaler.DemandRemoval, error) {
	observedDemand, err := handler.collectAvailableDemand(message.JobAvailableMessages)
	if err != nil {
		return nil, nil, err
	}
	removedDemand := make([]scaler.DemandRemoval, 0, len(message.JobStartedMessages))
	removedDemand = handler.collectStartedDemand(message.JobStartedMessages, removedDemand)
	return observedDemand, removedDemand, nil
}

func (handler coordinatedScaleSetHandler) collectAvailableDemand(messages []*upstream.JobAvailable) ([]scaler.JobDemand, error) {
	observedDemand := make([]scaler.JobDemand, 0, len(messages))
	for _, availableJob := range messages {
		if availableJob == nil || availableJob.RunnerRequestID <= 0 || availableJob.QueueTime.IsZero() {
			return nil, errors.New("job available message requires a positive runner request ID and queue time")
		}
		handler.logDemand("available", availableJob.RunnerRequestID, availableJob.QueueTime)
		observedDemand = append(observedDemand, scaler.JobDemand{
			RequestID:  availableJob.RunnerRequestID,
			Repository: handler.Repository,
			QueueTime:  availableJob.QueueTime,
		})
	}
	return observedDemand, nil
}

func (handler coordinatedScaleSetHandler) collectStartedDemand(
	messages []*upstream.JobStarted,
	removedDemand []scaler.DemandRemoval,
) []scaler.DemandRemoval {
	for _, startedJob := range messages {
		if startedJob == nil || startedJob.RunnerRequestID <= 0 {
			continue
		}
		removedDemand = append(removedDemand, scaler.DemandRemoval{Repository: handler.Repository, RequestID: startedJob.RunnerRequestID})
		handler.logDemandRemoval("started", startedJob.RunnerRequestID)
	}
	return removedDemand
}

func (handler coordinatedScaleSetHandler) completeJobs(ctx context.Context, messages []*upstream.JobCompleted) error {
	for _, completedJob := range messages {
		if completedJob == nil || (completedJob.RunnerRequestID <= 0 && completedJob.RunnerName == "") {
			continue
		}
		if err := handler.completeJob(ctx, completedJob); err != nil {
			return err
		}
		handler.logDemandRemoval("completed", completedJob.RunnerRequestID)
	}
	return nil
}

func (handler coordinatedScaleSetHandler) completeJob(ctx context.Context, completedJob *upstream.JobCompleted) error {
	if handler.ReapLibvirt {
		if err := handler.Coordinator.CompleteJob(ctx, scaler.JobCompletion{
			Repository: handler.Repository, RequestID: completedJob.RunnerRequestID, RunnerName: completedJob.RunnerName,
		}); err != nil {
			return fmt.Errorf("persist libvirt runner reap: %w", err)
		}
		return nil
	}
	if handler.Coordinator.HasRunnerLifecycle() {
		if err := handler.Coordinator.CompleteRunnerLifecycleJob(ctx, scaler.JobCompletion{
			Repository: handler.Repository, RequestID: completedJob.RunnerRequestID, RunnerName: completedJob.RunnerName,
		}, time.Now().UTC()); err != nil {
			return fmt.Errorf("persist runner cleanup: %w", err)
		}
		effectiveLogger(handler.Logger).Info("completed runner cleanup requested",
			slog.String("action", "runner_cleanup_request"), slog.String("phase", "completed"),
			slog.String("repository", handler.Repository), slog.String("scaleSet", handler.ScaleSet),
			slog.String("runner", completedJob.RunnerName), slog.Int64("requestID", completedJob.RunnerRequestID),
			slog.String("reason", "job_completed"),
		)
		return nil
	}
	if err := handler.Coordinator.Apply(ctx, nil, []scaler.DemandRemoval{{
		Repository: handler.Repository, RequestID: completedJob.RunnerRequestID,
	}}); err != nil {
		return fmt.Errorf("remove completed GitHub demand: %w", err)
	}
	return nil
}

func (handler coordinatedScaleSetHandler) markStartedRunnersBusy(ctx context.Context, messages []*upstream.JobStarted) error {
	for _, startedJob := range messages {
		if startedJob == nil || (startedJob.RunnerRequestID <= 0 && startedJob.RunnerName == "") {
			continue
		}
		err := handler.Coordinator.MarkRunnerBusy(
			ctx, handler.Repository, startedJob.RunnerRequestID, startedJob.RunnerName, int64(startedJob.RunnerID), time.Now().UTC(),
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("persist started runner lifecycle: %w", err)
		}
		effectiveLogger(handler.Logger).Info("runner marked busy",
			slog.String("action", "runner_lifecycle"), slog.String("phase", "busy"),
			slog.String("repository", handler.Repository), slog.String("scaleSet", handler.ScaleSet),
			slog.String("runner", startedJob.RunnerName), slog.Int64("requestID", startedJob.RunnerRequestID),
		)
	}
	return nil
}

func (handler coordinatedScaleSetHandler) logDemandRemoval(status string, requestID int64) {
	attributes := make([]any, 0, 6)
	attributes = append(attributes, slog.String("action", "demand_remove"), slog.String("phase", "completed"))
	attributes = append(attributes, jobLogAttributes(handler.Repository, handler.ScaleSet, requestID)...)
	attributes = append(attributes, slog.String("status", status))
	effectiveLogger(handler.Logger).Info("job demand removed", attributes...)
}

func (handler coordinatedScaleSetHandler) logDemand(status string, requestID int64, queueTime time.Time) {
	attributes := make([]any, 0, 7)
	attributes = append(attributes, slog.String("action", "demand_observe"), slog.String("phase", "completed"))
	attributes = append(attributes, jobLogAttributes(handler.Repository, handler.ScaleSet, requestID)...)
	attributes = append(attributes, slog.String("status", status), slog.Time("queueTime", queueTime))
	effectiveLogger(handler.Logger).Info("job demand observed", attributes...)
}

func jobLogAttributes(repository string, scaleSet string, requestID int64) []any {
	return []any{slog.String("repository", repository), slog.String("scaleSet", scaleSet), slog.Int64("requestID", requestID)}
}

func effectiveLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

func (handler scaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	_, err := handler.Reconciler.Reconcile(ctx, message.Statistics.TotalAssignedJobs)
	if err != nil {
		return fmt.Errorf("reconcile assigned jobs: %w", err)
	}
	return nil
}
