package execution

import (
	"context"
	"fmt"

	providerlibvirt "github.com/meza/RunnerFoundry/internal/provider/libvirt"
)

type LibvirtProvider struct {
	provider providerlibvirt.Provider
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
		ScaleSetName: request.RunnerPool,
		RunnerName:   request.RunnerName,
		JITConfig:    string(request.Bootstrap.content),
	})
	if err != nil {
		return fmt.Errorf("start libvirt runner: %w", err)
	}
	return nil
}

func (provider LibvirtProvider) Inspect(ctx context.Context, runnerPool string) ([]RunnerInstance, error) {
	instances, err := provider.provider.List(ctx, runnerPool)
	if err != nil {
		return nil, fmt.Errorf("inspect libvirt runners: %w", err)
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
