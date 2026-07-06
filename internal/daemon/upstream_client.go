//go:build !coverage

package daemon

import (
	"context"
	"fmt"

	upstream "github.com/actions/scaleset"
)

func newGitHubClient(repositoryURL string, token string) (upstreamScaleSetClient, error) {
	client, err := upstream.NewClientWithPersonalAccessToken(upstream.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     repositoryURL,
		PersonalAccessToken: token,
		SystemInfo: upstream.SystemInfo{
			System:  "github-scaler",
			Version: "v1",
		},
	})
	if err != nil {
		return upstreamScaleSetClient{}, fmt.Errorf("create scaleset client: %w", err)
	}
	return upstreamScaleSetClient{
		Client:   client,
		Registry: newGitHubScaleSetRegistry(repositoryURL, token),
	}, nil
}

type upstreamScaleSetClient struct {
	Client   *upstream.Client
	Registry scaleSetRegistry
}

func (client upstreamScaleSetClient) GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error) {
	runnerGroup, err := client.Client.GetRunnerGroupByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("get runner group by name: %w", err)
	}
	return runnerGroup, nil
}

func (client upstreamScaleSetClient) GetRunnerScaleSet(
	ctx context.Context,
	runnerGroupID int,
	scaleSetName string,
) (*upstream.RunnerScaleSet, error) {
	runnerScaleSet, err := client.Client.GetRunnerScaleSet(ctx, runnerGroupID, scaleSetName)
	if err != nil {
		return nil, fmt.Errorf("get runner scale set: %w", err)
	}
	return runnerScaleSet, nil
}

func (client upstreamScaleSetClient) CreateRunnerScaleSet(
	ctx context.Context,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	createdScaleSet, err := client.Client.CreateRunnerScaleSet(ctx, runnerScaleSet)
	if err != nil {
		return nil, fmt.Errorf("create runner scale set: %w", err)
	}
	return createdScaleSet, nil
}

func (client upstreamScaleSetClient) ListRunnerScaleSets(
	ctx context.Context,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	runnerScaleSets, err := client.Registry.ListRunnerScaleSets(ctx, runnerGroupID)
	if err != nil {
		return nil, fmt.Errorf("list runner scale sets: %w", err)
	}
	return runnerScaleSets, nil
}

func (client upstreamScaleSetClient) DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error {
	if err := client.Client.DeleteRunnerScaleSet(ctx, runnerScaleSetID); err != nil {
		return fmt.Errorf("delete runner scale set: %w", err)
	}
	return nil
}

func (client upstreamScaleSetClient) MessageSessionClient(
	ctx context.Context,
	runnerScaleSetID int,
	runnerName string,
) (scaleSetMessageClient, error) {
	messageClient, err := client.Client.MessageSessionClient(ctx, runnerScaleSetID, runnerName)
	if err != nil {
		return scaleSetMessageClient{}, fmt.Errorf("create message session client: %w", err)
	}
	return scaleSetMessageClient{Client: messageClient}, nil
}

func (client upstreamScaleSetClient) GenerateJitRunnerConfig(
	ctx context.Context,
	setting *upstream.RunnerScaleSetJitRunnerSetting,
	runnerScaleSetID int,
) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	jitConfig, err := client.Client.GenerateJitRunnerConfig(ctx, setting, runnerScaleSetID)
	if err != nil {
		return nil, fmt.Errorf("generate jit runner config: %w", err)
	}
	return jitConfig, nil
}

func (client upstreamScaleSetClient) GetRunnerByName(ctx context.Context, runnerName string) (*upstream.RunnerReference, error) {
	runner, err := client.Client.GetRunnerByName(ctx, runnerName)
	if err != nil {
		return nil, fmt.Errorf("get runner by name: %w", err)
	}
	return runner, nil
}

func (client upstreamScaleSetClient) RemoveRunner(ctx context.Context, runnerID int64) error {
	if err := client.Client.RemoveRunner(ctx, runnerID); err != nil {
		return fmt.Errorf("remove runner: %w", err)
	}
	return nil
}
