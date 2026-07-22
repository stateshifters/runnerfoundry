package daemon

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

func newGitHubClient(repositoryURL string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (githubScaleSetClient, error) {
	client, err := upstream.NewClientWithGitHubApp(upstream.ClientWithGitHubAppConfig{
		GitHubConfigURL: repositoryURL,
		GitHubAppAuth: upstream.GitHubAppAuth{
			ClientID:       strconv.FormatInt(credentials.AppID, 10),
			InstallationID: int64(installationID),
			PrivateKey:     credentials.PrivateKey,
		},
		SystemInfo: upstream.SystemInfo{
			System:  "github-scaler",
			Version: "v1",
		},
	})
	if err != nil {
		return githubScaleSetClient{}, fmt.Errorf("create scaleset client: %w", err)
	}
	if _, targetErr := parseRepositoryTarget(repositoryURL); targetErr != nil {
		return githubScaleSetClient{}, fmt.Errorf("validate repository target: %w", targetErr)
	}
	registry, err := newGitHubScaleSetRegistry(repositoryURL, installationID, credentials)
	if err != nil {
		return githubScaleSetClient{}, fmt.Errorf("create scaleset registry: %w", err)
	}
	return githubScaleSetClient{
		Client:                     client,
		Registry:                   registry,
		CreateMessageSessionClient: client.MessageSessionClient,
	}, nil
}

type githubScaleSetClient struct {
	*upstream.Client
	Registry                   scaleSetRegistry
	CreateMessageSessionClient func(
		ctx context.Context,
		runnerScaleSetID int,
		owner string,
		options ...upstream.HTTPOption,
	) (*upstream.MessageSessionClient, error)
}

func (client githubScaleSetClient) ListRunnerScaleSets(
	ctx context.Context,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	runnerScaleSets, err := client.Registry.ListRunnerScaleSets(ctx, runnerGroupID)
	if err != nil {
		return nil, fmt.Errorf("list runner scale sets: %w", err)
	}
	return runnerScaleSets, nil
}

func (client githubScaleSetClient) MessageSessionClient(
	ctx context.Context,
	runnerScaleSetID int,
	runnerName string,
) (scaleSetMessageClient, error) {
	messageClient, err := client.CreateMessageSessionClient(ctx, runnerScaleSetID, runnerName)
	if err != nil {
		return scaleSetMessageClient{}, fmt.Errorf("create message session client: %w", err)
	}
	return scaleSetMessageClient{Client: messageClient}, nil
}

type scaleSetClient interface {
	scaleSetSessionClient
	scaleSetDeletionClient
	scaleSetJITClient
}

type scaleSetDeletionClient interface {
	DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error
}

type scaleSetSessionClient interface {
	runnerGroupClient
	scaleSetRegistrationClient
	scaleSetMessageSessionClient
}

type runnerGroupClient interface {
	GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error)
}

type scaleSetRegistrationClient interface {
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, scaleSetName string) (*upstream.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, runnerScaleSet *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error)
}

type scaleSetMessageSessionClient interface {
	MessageSessionClient(ctx context.Context, runnerScaleSetID int, runnerName string) (scaleSetMessageClient, error)
}

type scaleSetJITClient interface {
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
	runnerGroupClient
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
}

type scaleSetMessageClient struct {
	Client messageSessionClient
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
	if message == nil {
		return nil, scalesetpoller.ErrNoMessage
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
