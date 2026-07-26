package daemon

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
)

type clusterMessageHandler struct {
	store     clusterDemandState
	scheduler *clusterScheduler
	session   *repositoryScaleSetSession
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
	_ = ctx
	for _, started := range messages {
		if started == nil || started.RunnerRequestID <= 0 {
			return errors.New("job started message requires a positive runner request ID")
		}
	}
	return nil
}

func (handler clusterMessageHandler) removeCompleted(ctx context.Context, messages []*upstream.JobCompleted) error {
	for _, completed := range messages {
		if completed == nil || completed.RunnerRequestID <= 0 {
			return errors.New("job completed message requires a positive runner request ID")
		}
		if err := handler.store.CompleteJob(ctx, cluster.JobCompletion{
			ScaleSet: handler.session.key.scaleSet, Repository: handler.session.key.repository,
			RequestID: completed.RunnerRequestID,
		}); err != nil {
			return fmt.Errorf("complete clustered job: %w", err)
		}
	}
	return nil
}
