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

// DockerComposeProvider adapts one validated Compose service to the
// source-neutral execution contracts.
type DockerComposeProvider struct {
	provider providerdocker.ComposeProvider
	hostname HostIdentity
}

func (provider DockerComposeProvider) EndpointID() InventoryEndpointID {
	config := provider.provider.Config
	return InventoryEndpointID(fmt.Sprintf("docker-compose:%s?compose=%s&service=%s",
		url.QueryEscape(strings.ToLower(strings.TrimSpace(string(provider.hostname)))),
		url.QueryEscape(filepath.Clean(strings.TrimSpace(config.ComposeFile))),
		url.QueryEscape(strings.TrimSpace(config.Service))))
}

func (provider DockerComposeProvider) KnowsEndpoint(endpointID InventoryEndpointID) bool {
	return provider.EndpointID() == endpointID
}

func (provider DockerComposeProvider) CanInspectEndpointForMember(
	endpointID InventoryEndpointID,
	memberID core.MemberID,
) bool {
	return provider.KnowsEndpoint(endpointID) &&
		strings.EqualFold(strings.TrimSpace(string(provider.hostname)), strings.TrimSpace(string(memberID)))
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

// NewDockerComposeProvider binds an explicit command transport, Compose
// configuration, and stable host identity. It performs no validation or I/O.
func NewDockerComposeProvider(
	transport providerdocker.CommandRunner,
	config providerdocker.ComposeConfig,
	hostname HostIdentity,
) DockerComposeProvider {
	return DockerComposeProvider{provider: providerdocker.ComposeProvider{
		CommandRunner: transport,
		Config:        config,
	}, hostname: hostname}
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
		RunnerName:     string(request.RunnerName),
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
			RunnerName: core.RunnerName(instance.RunnerName),
			RunnerPool: core.RunnerPool(instance.RunnerPoolName),
			State:      RunnerState(instance.State),
		})
	}
	return translated, nil
}

func (provider DockerComposeProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	err := provider.provider.Destroy(ctx, providerdocker.RunnerInstance{
		ID:         instance.ID,
		RunnerName: string(instance.RunnerName),
		State:      providerdocker.RunnerState(instance.State),
	})
	if err != nil {
		return fmt.Errorf("destroy Docker Compose runner: %w", err)
	}
	return nil
}
