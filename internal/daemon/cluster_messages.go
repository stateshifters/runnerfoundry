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

type clusterMessageHandler struct {
	store     clusterDemandState
	scheduler *clusterScheduler
	session   *repositoryScaleSetSession
	logger    *slog.Logger
}

const messageUpdateTimeout = 30 * time.Second

func (handler clusterMessageHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	if handler.session.assignedJobs == nil {
		return errors.New("cluster assigned-job recovery is required")
	}
	update, err := scaleSetMessageUpdate(
		handler.session.key.repository, handler.session.key.scaleSet, message, handler.observedAt(),
	)
	if err != nil {
		return err
	}
	update.AppID = handler.session.assignedJobs.appID
	update.ControllerEpoch = handler.session.assignedJobs.controllerEpoch
	updateCtx, cancelUpdate := context.WithTimeout(ctx, messageUpdateTimeout)
	result, err := handler.store.ApplyMessageUpdate(updateCtx, update)
	cancelUpdate()
	if err != nil {
		return fmt.Errorf("persist clustered scale-set message: %w", err)
	}
	handler.logCommittedDemand(ctx, update)
	handler.logCommittedCompletions(ctx, message.JobCompletedMessages)
	logCapacityObligationState(ctx, handler.logger, handler.session.assignedJobs.generation, update, result)
	handler.session.assignedJobs.remember(message.Statistics)
	return handler.scheduler.schedule(ctx, update.ScaleSet)
}

func logCapacityObligationState(
	ctx context.Context,
	logger *slog.Logger,
	generation uint64,
	update scaler.MessageUpdate,
	result scaler.MessageUpdateResult,
) {
	classification := scaler.ClassifyAssignedCapacity(scaler.AssignedCapacityStatistics{
		AssignedJobs: update.AssignedJobs, RunningJobs: update.RunningJobs,
		RegisteredRunners: update.RegisteredRunners, BusyRunners: update.BusyRunners,
	})
	attributes := capacityObligationQueueAttributes(update, result)
	attributes = append(attributes, capacityObligationStatisticsAttributes(update, classification)...)
	attributes = append(attributes,
		slog.Uint64("generation", generation), slog.Int64("epoch", update.ControllerEpoch),
	)
	if result.QueueSnapshot.OldestRunnableAt != nil {
		age := max(update.ObservedAt.Sub(*result.QueueSnapshot.OldestRunnableAt), time.Duration(0))
		attributes = append(attributes, slog.Duration("oldestRunnableAge", age))
	}
	logAction(ctx, logger, slog.LevelInfo, "capacity_obligation", "reconciled", attributes...)
	logStrandedAssignmentTransition(ctx, logger, result.AssignedObligations, attributes)
}

func capacityObligationQueueAttributes(
	update scaler.MessageUpdate,
	result scaler.MessageUpdateResult,
) []slog.Attr {
	return []slog.Attr{
		slog.String("repository", update.Repository),
		slog.String("scaleSet", update.ScaleSet),
		slog.Int("jobAvailable", result.QueueSnapshot.JobAvailable),
		slog.Int("assignedCount", result.QueueSnapshot.AssignedCount),
		slog.Int("newlyQueued", result.QueueSnapshot.NewlyQueued),
		slog.Int("canceled", result.QueueSnapshot.Canceled),
		slog.Int("createdAssigned", result.AssignedObligations.Create),
		slog.Int("canceledAssigned", result.AssignedObligations.Cancel),
		slog.Int("blockedAssigned", result.AssignedObligations.Block),
		slog.Int("unblockedAssigned", result.AssignedObligations.Unblock),
	}
}

func capacityObligationStatisticsAttributes(
	update scaler.MessageUpdate,
	classification scaler.AssignedCapacityClassification,
) []slog.Attr {
	return []slog.Attr{
		slog.Int("assignedJobs", update.AssignedJobs),
		slog.Int("runningJobs", update.RunningJobs),
		slog.Int("registeredRunners", update.RegisteredRunners),
		slog.Int("busyRunners", update.BusyRunners),
		slog.Int("strandedAssigned", classification.StrandedAssigned),
		slog.Int("runnableAssigned", classification.RunnableAssigned),
	}
}

func logStrandedAssignmentTransition(
	ctx context.Context,
	logger *slog.Logger,
	decision scaler.AssignedObligationDecision,
	attributes []slog.Attr,
) {
	if decision.Block > 0 {
		logAction(ctx, logger, slog.LevelWarn, "assigned_job_stranded", "blocked", attributes...)
	}
	if decision.Unblock > 0 {
		logAction(ctx, logger, slog.LevelInfo, "assigned_job_stranded", "recovered", attributes...)
	}
}

func (handler clusterMessageHandler) observedAt() time.Time {
	if handler.session != nil && handler.session.assignedJobs != nil && handler.session.assignedJobs.backend != nil &&
		handler.session.assignedJobs.backend.now != nil {
		return handler.session.assignedJobs.backend.now().UTC()
	}
	return time.Now().UTC()
}

func (handler clusterMessageHandler) logCommittedDemand(ctx context.Context, update scaler.MessageUpdate) {
	for _, demand := range update.Available {
		logAction(ctx, handler.logger, slog.LevelInfo, "demand_observe", "completed",
			slog.String("repository", update.Repository),
			slog.String("scaleSet", update.ScaleSet),
			slog.Int64("requestID", demand.RequestID),
			slog.Time("queueTime", demand.QueueTime),
			slog.Int64("epoch", update.ControllerEpoch),
		)
	}
}

func (handler clusterMessageHandler) logCommittedCompletions(
	ctx context.Context,
	completedMessages []*upstream.JobCompleted,
) {
	for _, completed := range completedMessages {
		if completed == nil || (completed.RunnerRequestID <= 0 && completed.RunnerName == "") {
			handler.warnInvalidCompletion(ctx, completed)
			continue
		}
		if completed.RunnerRequestID <= 0 {
			handler.logRunnerNameCompletion(ctx, completed)
		}
	}
}

func (handler clusterMessageHandler) logRunnerNameCompletion(ctx context.Context, completed *upstream.JobCompleted) {
	logAction(ctx, handler.logger, slog.LevelInfo, "cluster_job_completion", "completed",
		slog.String("repository", handler.session.key.repository),
		slog.String("scaleSet", handler.session.key.scaleSet),
		slog.String("messageType", string(upstream.MessageTypeJobCompleted)),
		slog.Int64("requestID", completed.RunnerRequestID),
		slog.String("runner", completed.RunnerName),
		slog.Int("runnerID", completed.RunnerID),
		slog.String("identity", "runner_name"),
	)
}

func (handler clusterMessageHandler) warnInvalidCompletion(ctx context.Context, completed *upstream.JobCompleted) {
	var requestID int64
	var runnerName string
	var runnerID int
	if completed != nil {
		requestID = completed.RunnerRequestID
		runnerName = completed.RunnerName
		runnerID = completed.RunnerID
	}
	logAction(ctx, handler.logger, slog.LevelWarn, "cluster_job_completion_invalid", "dropped",
		slog.String("repository", handler.session.key.repository),
		slog.String("scaleSet", handler.session.key.scaleSet),
		slog.String("messageType", string(upstream.MessageTypeJobCompleted)),
		slog.Int64("requestID", requestID),
		slog.String("runner", runnerName),
		slog.Int("runnerID", runnerID),
	)
}
