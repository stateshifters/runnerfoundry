package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/scaler"
)

type scaleSetHandler struct {
	Reconciler scaler.Reconciler
}

type coordinatedScaleSetHandler struct {
	Coordinator *scaler.Coordinator
	Repository  string
	Logger      *slog.Logger
	ScaleSet    string
}

func (handler coordinatedScaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	observedDemand, removedDemand, err := handler.collectDemand(message)
	if err != nil {
		return err
	}
	if applyErr := handler.Coordinator.Apply(ctx, observedDemand, removedDemand); applyErr != nil {
		return fmt.Errorf("persist GitHub job demand: %w", applyErr)
	}
	reconciliation, err := handler.Coordinator.ReconcileAssigned(ctx, handler.Repository, message.Statistics.TotalAssignedJobs)
	if err != nil {
		return fmt.Errorf("schedule shared runner capacity: %w", err)
	}
	effectiveLogger(handler.Logger).Info(
		"assigned capacity reconciled",
		slog.String("action", "capacity_reconcile"),
		slog.String("phase", "completed"),
		slog.String("repository", handler.Repository),
		slog.String("scaleSet", handler.ScaleSet),
		slog.Int("assignedJobs", reconciliation.AssignedJobs),
		slog.Int("runningRunners", reconciliation.RunningRunners),
		slog.Int("startedRunners", reconciliation.StartedRunners),
	)
	return nil
}

func (handler coordinatedScaleSetHandler) collectDemand(message *upstream.RunnerScaleSetMessage) ([]scaler.JobDemand, []scaler.DemandRemoval, error) {
	observedDemand, err := handler.collectAvailableDemand(message.JobAvailableMessages)
	if err != nil {
		return nil, nil, err
	}
	removedDemand := make([]scaler.DemandRemoval, 0, len(message.JobStartedMessages)+len(message.JobCompletedMessages))
	removedDemand = handler.collectStartedDemand(message.JobStartedMessages, removedDemand)
	removedDemand = handler.collectCompletedDemand(message.JobCompletedMessages, removedDemand)
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

func (handler coordinatedScaleSetHandler) collectCompletedDemand(
	messages []*upstream.JobCompleted,
	removedDemand []scaler.DemandRemoval,
) []scaler.DemandRemoval {
	for _, completedJob := range messages {
		if completedJob == nil || completedJob.RunnerRequestID <= 0 {
			continue
		}
		removedDemand = append(removedDemand, scaler.DemandRemoval{Repository: handler.Repository, RequestID: completedJob.RunnerRequestID})
		handler.logDemandRemoval("completed", completedJob.RunnerRequestID)
	}
	return removedDemand
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
