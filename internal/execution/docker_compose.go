package execution

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/meza/RunnerFoundry/internal/core"
	providerdocker "github.com/meza/RunnerFoundry/internal/provider/docker"
)

type DockerComposeProvider struct {
	provider providerdocker.ComposeProvider
	hostname string
}

func (provider DockerComposeProvider) EndpointID() InventoryEndpointID {
	config := provider.provider.Config
	return InventoryEndpointID(fmt.Sprintf("docker-compose:%s?compose=%s&service=%s",
		url.QueryEscape(strings.ToLower(strings.TrimSpace(provider.hostname))),
		url.QueryEscape(filepath.Clean(strings.TrimSpace(config.ComposeFile))),
		url.QueryEscape(strings.TrimSpace(config.Service))))
}

func (provider DockerComposeProvider) KnowsEndpoint(endpointID InventoryEndpointID) bool {
	return provider.EndpointID() == endpointID
}

func (provider DockerComposeProvider) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}

func (provider DockerComposeProvider) InspectEndpoint(ctx context.Context, endpointID InventoryEndpointID) ([]RunnerInstance, error) {
	if !provider.KnowsEndpoint(endpointID) {
		return nil, fmt.Errorf("docker Compose endpoint %q is not configured", endpointID)
	}
	return provider.Inspect(ctx)
}

func NewDockerComposeProvider(
	transport providerdocker.CommandRunner,
	config providerdocker.ComposeConfig,
	hostname ...string,
) DockerComposeProvider {
	memberHostname := "local"
	if len(hostname) != 0 && hostname[0] != "" {
		memberHostname = hostname[0]
	}
	return DockerComposeProvider{provider: providerdocker.ComposeProvider{
		CommandRunner: transport,
		Config:        config,
	}, hostname: memberHostname}
}

func (provider DockerComposeProvider) Validate(ctx context.Context) error {
	if err := provider.provider.Validate(ctx); err != nil {
		return fmt.Errorf("validate Docker Compose provider: %w", err)
	}
	return nil
}

func (provider DockerComposeProvider) Start(ctx context.Context, request StartRequest) error {
	err := provider.provider.Start(ctx, providerdocker.StartRequest{
		RunnerPoolName: string(request.RunnerPool),
		RunnerName:     request.RunnerName,
		JITConfig:      string(request.Bootstrap.content),
	})
	if err != nil {
		return fmt.Errorf("start Docker Compose runner: %w", err)
	}
	return nil
}

func (provider DockerComposeProvider) Inspect(ctx context.Context) ([]RunnerInstance, error) {
	instances, err := provider.provider.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker Compose runners: %w", err)
	}
	translated := make([]RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		translated = append(translated, RunnerInstance{
			ID:         instance.ID,
			RunnerName: instance.RunnerName,
			RunnerPool: core.RunnerPool(instance.RunnerPoolName),
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
