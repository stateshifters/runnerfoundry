//go:build coverage

package daemon

import (
	"context"
	"errors"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func newGitHubClient(loadedConfig config.Config, token string) (coverageScaleSetClient, error) {
	if token == "" {
		return coverageScaleSetClient{}, errors.New("github token is required")
	}
	return coverageScaleSetClient{}, nil
}

type coverageScaleSetClient struct{}

func (client coverageScaleSetClient) GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error) {
	return nil, errors.New("coverage client has no GitHub transport")
}

func (client coverageScaleSetClient) GetRunnerScaleSet(
	ctx context.Context,
	runnerGroupID int,
	scaleSetName string,
) (*upstream.RunnerScaleSet, error) {
	return nil, errors.New("coverage client has no GitHub transport")
}

func (client coverageScaleSetClient) CreateRunnerScaleSet(
	ctx context.Context,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	return nil, errors.New("coverage client has no GitHub transport")
}

func (client coverageScaleSetClient) MessageSessionClient(
	ctx context.Context,
	runnerScaleSetID int,
	runnerName string,
) (scaleSetMessageClient, error) {
	return scaleSetMessageClient{}, errors.New("coverage client has no GitHub transport")
}

func (client coverageScaleSetClient) GenerateJitRunnerConfig(
	ctx context.Context,
	setting *upstream.RunnerScaleSetJitRunnerSetting,
	runnerScaleSetID int,
) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	return nil, errors.New("coverage client has no GitHub transport")
}
