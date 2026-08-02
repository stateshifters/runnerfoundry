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
	return handler.scheduler.schedule(ctx, handler.session.key.scaleSet)
}

func (handler clusterMessageHandler) enqueueAvailable(ctx context.Context, messages []*upstream.JobAvailable) error {
	for _, available := range messages {
		if available == nil || available.RunnerRequestID <= 0 || available.QueueTime.IsZero() {
			return errors.New("job available message requires a positive runner request ID and queue time")
		}
		if err := handler.store.Enqueue(ctx, cluster.Demand{
			ScaleSet: handler.session.key.scaleSet, Repository: handler.session.key.repository,
			RequestID: available.RunnerRequestID, QueueTime: available.QueueTime,
		}); err != nil {
			return fmt.Errorf("persist clustered job demand: %w", err)
		}
	}
	return nil
}

func (handler clusterMessageHandler) removeStarted(ctx context.Context, messages []*upstream.JobStarted) error {
	for _, started := range messages {
		if started == nil || started.RunnerRequestID <= 0 {
			return errors.New("job started message requires a positive runner request ID")
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
	return handler.reconcileInvalidCompletion(ctx, completed)
}

func (handler clusterMessageHandler) reconcileInvalidCompletion(ctx context.Context, completed *upstream.JobCompleted) error {
	if completed.RunnerName != "" {
		if err := handler.completeJob(ctx, completed); err != nil {
			return err
		}
	}
	handler.warnInvalidCompletion(ctx, completed)
	return nil
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
