package daemon

import (
	"context"
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

func jobStartedHasIdentity(startedJob *upstream.JobStarted) bool {
	return startedJob != nil && (startedJob.RunnerRequestID > 0 || startedJob.RunnerName != "")
}

func (handler coordinatedScaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	logScaleSetSessionStatistics(
		ctx, handler.Logger, handler.Repository, handler.ScaleSet, "message", message.Statistics,
	)
	update, err := scaleSetMessageUpdate(handler.Repository, handler.ScaleSet, message, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("apply atomic scale-set message: %w", err)
	}
	updateCtx, cancelUpdate := context.WithTimeout(ctx, messageUpdateTimeout)
	result, err := handler.Coordinator.ApplyMessageUpdate(updateCtx, update)
	cancelUpdate()
	if err != nil {
		return fmt.Errorf("persist atomic scale-set message: %w", err)
	}
	for _, demand := range update.Available {
		handler.logDemand("available", demand.RequestID, demand.QueueTime)
	}
	if err = handler.Coordinator.Schedule(ctx); err != nil {
		return fmt.Errorf("schedule shared runner capacity: %w", err)
	}
	handler.logAssignedCapacity(message.Statistics.TotalAssignedJobs, 0,
		result.AssignedObligations.Create, result.AssignedObligations.Cancel)
	return nil
}

func (handler coordinatedScaleSetHandler) logAssignedCapacity(
	assignedJobs int,
	startedRunners int,
	queuedObligations int,
	canceledObligations int,
) {
	effectiveLogger(handler.Logger).Info(
		"assigned capacity reconciled",
		slog.String("action", "assigned_recovery"),
		slog.String("phase", "completed"),
		slog.String("repository", handler.Repository),
		slog.String("scaleSet", handler.ScaleSet),
		slog.Int("assignedJobs", assignedJobs),
		slog.Int("startedRunners", startedRunners),
		slog.Int("queuedObligations", queuedObligations),
		slog.Int("canceledObligations", canceledObligations),
	)
}

func logScaleSetSessionStatistics(
	ctx context.Context,
	logger *slog.Logger,
	repository string,
	scaleSet string,
	trigger string,
	statistics *upstream.RunnerScaleSetStatistic,
) {
	logAction(ctx, logger, slog.LevelInfo, "session_statistics", "observed",
		slog.String("trigger", trigger), slog.String("repository", repository), slog.String("scaleSet", scaleSet),
		slog.Int("assignedJobs", statistics.TotalAssignedJobs), slog.Int("availableJobs", statistics.TotalAvailableJobs),
		slog.Int("acquiredJobs", statistics.TotalAcquiredJobs), slog.Int("runningJobs", statistics.TotalRunningJobs),
		slog.Int("registeredRunners", statistics.TotalRegisteredRunners), slog.Int("busyRunners", statistics.TotalBusyRunners),
		slog.Int("idleRunners", statistics.TotalIdleRunners))
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
