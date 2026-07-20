package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	upstream "github.com/actions/scaleset"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

const pollRetryDelay = 5 * time.Second
const sustainedPollFailureThreshold = 3

var operatingSystemHostname = os.Hostname

type scaleSetClient interface {
	scaleSetAdministrationClient
	scaleSetRunnerClient
}

type scaleSetDeletionClient interface {
	DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error
}

type scaleSetAdministrationClient interface {
	scaleSetDeletionClient
	GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error)
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, scaleSetName string) (*upstream.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, runnerScaleSet *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error)
}

type scaleSetRunnerClient interface {
	MessageSessionClient(ctx context.Context, runnerScaleSetID int, runnerName string) (scaleSetMessageClient, error)
	GenerateJitRunnerConfig(
		ctx context.Context,
		setting *upstream.RunnerScaleSetJitRunnerSetting,
		runnerScaleSetID int,
	) (*upstream.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(ctx context.Context, runnerName string) (*upstream.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

type scaleSetPruneClient interface {
	scaleSetDeletionClient
	GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error)
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
}

type scaleSetMessageClient struct {
	Client messageSessionClient
}

type scaleSetLifecycle struct {
	StopPolling   func()
	Drain         func(ctx context.Context) (int, error)
	DestroyActive func(ctx context.Context) error
	Shutdown      func(ctx context.Context) error
}

type messageSessionClient interface {
	scalesetpoller.MessageClient
	Close(ctx context.Context) error
	Session() upstream.RunnerScaleSetSession
}

func (client scaleSetMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	message, err := client.Client.GetMessage(ctx, lastMessageID, maxCapacity)
	if err != nil {
		return nil, fmt.Errorf("get message: %w", err)
	}
	return message, nil
}

func (client scaleSetMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	if err := client.Client.DeleteMessage(ctx, messageID); err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

func (client scaleSetMessageClient) Close(ctx context.Context) error {
	if err := client.Client.Close(ctx); err != nil {
		return fmt.Errorf("close message session: %w", err)
	}
	return nil
}

func (client scaleSetMessageClient) initialStatistics() *upstream.RunnerScaleSetStatistic {
	return client.Client.Session().Statistics
}

func (client scaleSetMessageClient) Acquire(ctx context.Context, requestID int64) error {
	acquirer, ok := client.Client.(interface {
		AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error)
	})
	if !ok {
		return errors.New("message session does not support job acquisition")
	}
	acquiredIDs, err := acquirer.AcquireJobs(ctx, []int64{requestID})
	if err != nil {
		return fmt.Errorf("acquire job: %w", err)
	}
	for _, acquiredID := range acquiredIDs {
		if acquiredID == requestID {
			return nil
		}
	}
	return fmt.Errorf("job %d was not acquired", requestID)
}
