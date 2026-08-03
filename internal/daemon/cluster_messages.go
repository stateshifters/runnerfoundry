package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type clusterMessageHandler struct {
	store     clusterDemandState
	scheduler *clusterScheduler
	session   *repositoryScaleSetSession
	logger    *slog.Logger
}

func (handler clusterMessageHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	if err := handler.enqueueAvailable(ctx, message.JobAvailableMessages); err != nil {
		return err
	}
	if err := handler.removeStarted(ctx, message.JobStartedMessages); err != nil {
		return err
	}
	if err := handler.removeCompleted(ctx, message.JobCompletedMessages); err != nil {
		return err
	}
	if err := handler.scheduler.schedule(ctx, handler.session.key.scaleSet); err != nil {
		return err
	}
	if handler.session.assignedJobs == nil {
		return errors.New("cluster assigned-job recovery is required")
	}
	return handler.session.assignedJobs.observe(ctx, message.Statistics, "message")
}

func (handler clusterMessageHandler) enqueueAvailable(ctx context.Context, messages []*upstream.JobAvailable) error {
	for _, available := range messages {
		if available == nil || available.RunnerRequestID <= 0 || available.QueueTime.IsZero() {
			return errors.New("job available message requires a positive runner request ID and queue time")
		}
		if err := handler.store.Enqueue(ctx, cluster.Demand{
			AppID:    handler.session.assignedJobs.appID,
			ScaleSet: handler.session.key.scaleSet, Repository: handler.session.key.repository,
			RequestID: available.RunnerRequestID, QueueTime: available.QueueTime,
		}); err != nil {
			return fmt.Errorf("persist clustered job demand: %w", err)
		}
		epoch := int64(0)
		if handler.session.assignedJobs != nil {
			epoch = handler.session.assignedJobs.controllerEpoch
		}
		logAction(ctx, handler.logger, slog.LevelInfo, "demand_observe", "completed",
			slog.String("repository", handler.session.key.repository),
			slog.String("scaleSet", handler.session.key.scaleSet),
			slog.Int64("requestID", available.RunnerRequestID),
			slog.Time("queueTime", available.QueueTime),
			slog.Int64("epoch", epoch),
		)
	}
	return nil
}

func (handler clusterMessageHandler) removeStarted(ctx context.Context, messages []*upstream.JobStarted) error {
	for _, started := range messages {
		if !jobStartedHasIdentity(started) {
			return errors.New("job started message requires a positive runner request ID or runner name")
		}
		if lifecycleStore, supported := handler.store.(scaler.RunnerLifecycleStore); supported {
			err := lifecycleStore.MarkRunnerBusy(
				ctx, handler.session.key.scaleSet, handler.session.key.repository,
				started.RunnerRequestID, started.RunnerName, int64(started.RunnerID), time.Now().UTC(),
			)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("persist clustered started runner lifecycle: %w", err)
			}
		}
	}
	return nil
}

func (handler clusterMessageHandler) removeCompleted(ctx context.Context, messages []*upstream.JobCompleted) error {
	for _, completed := range messages {
		if err := handler.handleCompleted(ctx, completed); err != nil {
			return err
		}
	}
	return nil
}

func (handler clusterMessageHandler) handleCompleted(ctx context.Context, completed *upstream.JobCompleted) error {
	if completed == nil {
		handler.warnInvalidCompletion(ctx, nil)
		return nil
	}
	if completed.RunnerRequestID > 0 {
		return handler.completeJob(ctx, completed)
	}
	return handler.reconcileIdentityFreeCompletion(ctx, completed)
}

func (handler clusterMessageHandler) reconcileIdentityFreeCompletion(ctx context.Context, completed *upstream.JobCompleted) error {
	if completed.RunnerName != "" {
		if err := handler.completeJob(ctx, completed); err != nil {
			return err
		}
		handler.logRunnerNameCompletion(ctx, completed)
		return nil
	}
	handler.warnInvalidCompletion(ctx, completed)
	return nil
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

func (handler clusterMessageHandler) completeJob(ctx context.Context, completed *upstream.JobCompleted) error {
	if err := handler.store.CompleteJob(ctx, cluster.JobCompletion{
		ScaleSet: handler.session.key.scaleSet, Repository: handler.session.key.repository,
		RequestID: completed.RunnerRequestID, RunnerName: completed.RunnerName,
	}); err != nil {
		return fmt.Errorf("complete clustered job: %w", err)
	}
	return nil
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
