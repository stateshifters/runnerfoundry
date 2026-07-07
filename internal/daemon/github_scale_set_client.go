package daemon

import (
	"context"
	"fmt"

	upstream "github.com/actions/scaleset"
)

func newGitHubClient(repositoryURL string, token string) (githubScaleSetClient, error) {
	client, err := upstream.NewClientWithPersonalAccessToken(upstream.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     repositoryURL,
		PersonalAccessToken: token,
		SystemInfo: upstream.SystemInfo{
			System:  "github-scaler",
			Version: "v1",
		},
	})
	if err != nil {
		return githubScaleSetClient{}, fmt.Errorf("create scaleset client: %w", err)
	}
	return githubScaleSetClient{
		Client:                     client,
		Registry:                   newGitHubScaleSetRegistry(repositoryURL, token),
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
