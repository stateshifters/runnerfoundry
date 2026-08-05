package execution

import (
	"context"
	"fmt"

	providerdocker "github.com/meza/RunnerFoundry/internal/provider/docker"
)

type DockerComposeProvider struct {
	provider providerdocker.ComposeProvider
}

func NewDockerComposeProvider(
	transport providerdocker.CommandRunner,
	config providerdocker.ComposeConfig,
) DockerComposeProvider {
	return DockerComposeProvider{provider: providerdocker.ComposeProvider{
		CommandRunner: transport,
		Config:        config,
	}}
}

func (provider DockerComposeProvider) Validate(ctx context.Context) error {
	if err := provider.provider.Validate(ctx); err != nil {
		return fmt.Errorf("validate Docker Compose provider: %w", err)
	}
	return nil
}

func (provider DockerComposeProvider) Start(ctx context.Context, request StartRequest) error {
	err := provider.provider.Start(ctx, providerdocker.StartRequest{
		ScaleSetName: request.RunnerPool,
		RunnerName:   request.RunnerName,
		JITConfig:    string(request.Bootstrap.content),
	})
	if err != nil {
		return fmt.Errorf("start Docker Compose runner: %w", err)
	}
	return nil
}

func (provider DockerComposeProvider) Inspect(
	ctx context.Context,
	runnerPool string,
) ([]RunnerInstance, error) {
	instances, err := provider.provider.List(ctx, runnerPool)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker Compose runners: %w", err)
	}
	translated := make([]RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		translated = append(translated, RunnerInstance{
			ID:         instance.ID,
			RunnerName: instance.RunnerName,
			State:      RunnerState(instance.State),
		})
	}
	return translated, nil
}

func (provider DockerComposeProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	err := provider.provider.Destroy(ctx, providerdocker.RunnerInstance{
		ID:         instance.ID,
		RunnerName: instance.RunnerName,
		State:      providerdocker.RunnerState(instance.State),
	})
	if err != nil {
		return fmt.Errorf("destroy Docker Compose runner: %w", err)
	}
	return nil
}
