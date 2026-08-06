package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/execution"
	dockerprovider "github.com/meza/RunnerFoundry/internal/provider/docker"
	libvirtprovider "github.com/meza/RunnerFoundry/internal/provider/libvirt"
)

type executionDependencies struct {
	dockerCommands  dockerprovider.CommandRunner
	libvirtCommands libvirtprovider.CommandRunner
	readTemplate    func(string) ([]byte, error)
	memberHostname  string
}

type configuredExecutionProvider struct{ execution.ExecutionProvider }

func buildExecutionProvider(
	ctx context.Context,
	configured config.ScaleSetConfig,
	dependencies executionDependencies,
) (configuredExecutionProvider, error) {
	provider, err := configuredProvider(configured, dependencies)
	if err != nil {
		return configuredExecutionProvider{}, err
	}
	if err = provider.Validate(ctx); err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("validate execution provider for Runner Pool %q: %w", configured.Name, err)
	}
	return provider, nil
}

func configuredProvider(
	configured config.ScaleSetConfig,
	dependencies executionDependencies,
) (configuredExecutionProvider, error) {
	kind, err := configured.ProviderKind()
	if err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("runner pool %q: %w", configured.Name, err)
	}
	if kind == config.ProviderKindDocker {
		provider := execution.NewDockerComposeProvider(
			dependencies.dockerCommands,
			dockerprovider.ComposeConfig{
				ComposeFile: configured.Docker.ComposeFile,
				Service:     configured.Docker.Service,
			}, dependencies.memberHostname,
		)
		return configuredExecutionProvider{ExecutionProvider: provider}, nil
	}
	if dependencies.readTemplate == nil {
		return configuredExecutionProvider{}, errors.New("libvirt template reader is required")
	}
	template, err := dependencies.readTemplate(configured.Libvirt.DomainXMLTemplate)
	if err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("read libvirt domain template: %w", err)
	}
	provider := execution.NewLibvirtProvider(
		dependencies.libvirtCommands,
		libvirtprovider.Config{
			ConnectionURI:     configured.Libvirt.ConnectionURI,
			DomainXMLTemplate: configured.Libvirt.DomainXMLTemplate,
		},
		template,
	)
	return configuredExecutionProvider{ExecutionProvider: provider}, nil
}
