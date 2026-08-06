package execution

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/meza/RunnerFoundry/internal/core"
	providerlibvirt "github.com/meza/RunnerFoundry/internal/provider/libvirt"
)

type LibvirtProvider struct {
	provider providerlibvirt.Provider
}

func (provider LibvirtProvider) EndpointID() InventoryEndpointID {
	return InventoryEndpointID("libvirt:" + normalizedLibvirtURI(provider.provider.Config.ConnectionURI))
}

func normalizedLibvirtURI(raw string) string {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String()
}

func (provider LibvirtProvider) KnowsEndpoint(endpointID InventoryEndpointID) bool {
	return provider.EndpointID() == endpointID
}

func (provider LibvirtProvider) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}

func (provider LibvirtProvider) InspectEndpoint(ctx context.Context, endpointID InventoryEndpointID) ([]RunnerInstance, error) {
	if !provider.KnowsEndpoint(endpointID) {
		return nil, fmt.Errorf("libvirt endpoint %q is not configured", endpointID)
	}
	return provider.Inspect(ctx)
}

func NewLibvirtProvider(
	transport providerlibvirt.CommandRunner,
	config providerlibvirt.Config,
	domainTemplate []byte,
) LibvirtProvider {
	template := append([]byte(nil), domainTemplate...)
	return LibvirtProvider{provider: providerlibvirt.NewProviderWithTemplateReader(
		transport,
		config,
		func(string) ([]byte, error) { return append([]byte(nil), template...), nil },
	)}
}

func (provider LibvirtProvider) Validate(ctx context.Context) error {
	if err := provider.provider.Validate(ctx); err != nil {
		return fmt.Errorf("validate libvirt provider: %w", err)
	}
	return nil
}

func (provider LibvirtProvider) Start(ctx context.Context, request StartRequest) error {
	err := provider.provider.Start(ctx, providerlibvirt.StartRequest{
		RunnerPoolName: string(request.RunnerPool),
		RunnerName:     request.RunnerName,
		JITConfig:      string(request.Bootstrap.content),
	})
	if err != nil {
		return fmt.Errorf("start libvirt runner: %w", err)
	}
	return nil
}

func (provider LibvirtProvider) Inspect(ctx context.Context) ([]RunnerInstance, error) {
	instances, err := provider.provider.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect libvirt runners: %w", err)
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

func (provider LibvirtProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	err := provider.provider.Destroy(ctx, providerlibvirt.RunnerInstance{
		ID:         instance.ID,
		RunnerName: instance.RunnerName,
		State:      providerlibvirt.RunnerState(instance.State),
	})
	if err != nil {
		return fmt.Errorf("destroy libvirt runner: %w", err)
	}
	return nil
}
