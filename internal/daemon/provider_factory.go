package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	libvirtprovider "github.com/stateshifters/runnerfoundry/internal/provider/libvirt"
)

type executionDependencies struct {
	newDockerLauncher  func(dockerprovider.ComposeConfig, core.MemberID) execution.RunnerLauncher
	newDockerInventory func(core.MemberID) execution.RunnerInventory
	libvirtCommands    libvirtprovider.CommandRunner
	readTemplate       func(string) ([]byte, error)
	memberID           core.MemberID
	providerObserver   func(execution.ProviderActivity)
}

type configuredExecutionProvider struct {
	Launcher  execution.RunnerLauncher
	Inventory execution.RunnerInventory
}

func buildExecutionProvider(
	ctx context.Context,
	configured config.RunnerPoolConfig,
	dependencies executionDependencies,
) (configuredExecutionProvider, error) {
	provider, err := configuredProvider(configured, dependencies)
	if err != nil {
		return configuredExecutionProvider{}, err
	}
	if err = provider.Launcher.Validate(ctx); err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("validate execution provider for Runner Pool %q: %w", configured.Name, err)
	}
	return provider, nil
}

func configuredProvider(
	configured config.RunnerPoolConfig,
	dependencies executionDependencies,
) (configuredExecutionProvider, error) {
	kind, err := configured.ProviderKind()
	if err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("runner pool %q: %w", configured.Name, err)
	}
	if kind == config.ProviderKindDocker {
		return configuredDockerProvider(configured, dependencies)
	}
	return configuredLibvirtProvider(configured, dependencies)
}

func configuredDockerProvider(
	configured config.RunnerPoolConfig,
	dependencies executionDependencies,
) (configuredExecutionProvider, error) {
	if dependencies.newDockerLauncher == nil {
		return configuredExecutionProvider{}, errors.New("docker launcher factory is required")
	}
	launcher := dependencies.newDockerLauncher(
		dockerprovider.ComposeConfig{
			ComposeFile: configured.Docker.ComposeFile,
			Service:     configured.Docker.Service,
		}, dependencies.memberID,
	)
	return configuredExecutionProvider{
		Launcher: execution.WithLauncherOperationDeadlinesAndMetadata(
			launcher, execution.ProviderMetadata{Name: execution.ProviderName(config.ProviderKindDocker),
				ComposeService: configured.Docker.Service}, dependencies.providerObserver,
		),
	}, nil
}

func configuredDockerInventory(dependencies executionDependencies) (execution.InventoryOperationDeadlineProvider, error) {
	if dependencies.newDockerInventory == nil {
		return execution.InventoryOperationDeadlineProvider{}, errors.New("docker inventory factory is required")
	}
	inventory := dependencies.newDockerInventory(dependencies.memberID)
	return execution.WithInventoryOperationDeadlines(
		inventory, execution.ProviderName(config.ProviderKindDocker), dependencies.providerObserver,
	), nil
}

func configuredLibvirtProvider(
	configured config.RunnerPoolConfig,
	dependencies executionDependencies,
) (configuredExecutionProvider, error) {
	if dependencies.readTemplate == nil {
		return configuredExecutionProvider{}, errors.New("libvirt template reader is required")
	}
	template, err := dependencies.readTemplate(configured.Libvirt.DomainXMLTemplate)
	if err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("read libvirt domain template: %w", err)
	}
	provider := libvirtprovider.NewProvider(
		dependencies.libvirtCommands,
		libvirtprovider.Config{
			ConnectionURI:     configured.Libvirt.ConnectionURI,
			DomainXMLTemplate: configured.Libvirt.DomainXMLTemplate,
		},
		template,
		dependencies.memberID,
	)
	wrapped := execution.WithOperationDeadlines(
		provider, execution.ProviderName(config.ProviderKindLibvirt), dependencies.providerObserver,
	)
	return configuredExecutionProvider{Launcher: wrapped, Inventory: wrapped}, nil
}
