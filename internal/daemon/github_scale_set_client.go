package daemon

import (
	"context"
	"fmt"
	"strconv"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func newGitHubClient(repositoryURL string, installationID int64, credentials config.GitHubAppCredentials) (githubScaleSetClient, error) {
	client, err := upstream.NewClientWithGitHubApp(upstream.ClientWithGitHubAppConfig{
		GitHubConfigURL: repositoryURL,
		GitHubAppAuth: upstream.GitHubAppAuth{
			ClientID:       strconv.FormatInt(credentials.AppID, 10),
			InstallationID: installationID,
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
