package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	libvirtprovider "github.com/stateshifters/runnerfoundry/internal/provider/libvirt"
	orchardprovider "github.com/stateshifters/runnerfoundry/internal/provider/orchard"
	runtimeservice "github.com/stateshifters/runnerfoundry/internal/runtime"
)

type executionDependencies struct {
	newOrchardProvider func(orchardprovider.Config) (execution.ExecutionProvider, error)
	newDockerLauncher  func(dockerprovider.ComposeConfig, core.MemberID) execution.RunnerLauncher
	newDockerInventory func(core.MemberID) execution.RunnerInventory
	libvirtCommands    libvirtprovider.CommandRunner
	readTemplate       func(string) ([]byte, error)
	readFile           config.ReadFile
	memberID           core.MemberID
	providerObserver   providerActivityObserver
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
	var provider configuredExecutionProvider
	switch kind {
	case config.ProviderKindDocker:
		provider, err = configuredDockerProvider(configured, dependencies)
	case config.ProviderKindLibvirt:
		provider, err = configuredLibvirtProvider(configured, dependencies)
	case config.ProviderKindOrchard:
		provider, err = configuredOrchardProvider(configured, dependencies)
	}
	return provider, err
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
		Launcher: runtimeservice.WithLauncherOperationDeadlines(
			launcher, runtimeservice.ProviderName(config.ProviderKindDocker),
			observeProviderActivity(dependencies.providerObserver, configured.Docker.Service),
		),
	}, nil
}

func configuredDockerInventory(dependencies executionDependencies) (runtimeservice.InventoryOperationDeadlineProvider, error) {
	if dependencies.newDockerInventory == nil {
		return runtimeservice.InventoryOperationDeadlineProvider{}, errors.New("docker inventory factory is required")
	}
	inventory := dependencies.newDockerInventory(dependencies.memberID)
	return runtimeservice.WithInventoryOperationDeadlines(
		inventory, runtimeservice.ProviderName(config.ProviderKindDocker),
		observeProviderActivity(dependencies.providerObserver, ""),
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
	wrapped := runtimeservice.WithOperationDeadlines(
		provider, runtimeservice.ProviderName(config.ProviderKindLibvirt),
		observeProviderActivity(dependencies.providerObserver, ""),
	)
	return configuredExecutionProvider{Launcher: wrapped, Inventory: wrapped}, nil
}

func configuredOrchardProvider(configured config.RunnerPoolConfig, dependencies executionDependencies) (configuredExecutionProvider, error) {
	if dependencies.newOrchardProvider == nil {
		return configuredExecutionProvider{}, errors.New("orchard provider factory is required")
	}
	if dependencies.readFile == nil {
		return configuredExecutionProvider{}, errors.New("orchard file reader is required")
	}
	token, err := dependencies.readFile(configured.Orchard.TokenFile)
	if err != nil {
		return configuredExecutionProvider{}, errors.New("read Orchard token file")
	}
	spec, err := dependencies.readFile(configured.Orchard.VMSpecFile)
	if err != nil {
		return configuredExecutionProvider{}, errors.New("read Orchard VM specification file")
	}
	var certificate []byte
	if configured.Orchard.TrustedCertificateFile != "" {
		certificate, err = dependencies.readFile(configured.Orchard.TrustedCertificateFile)
		if err != nil {
			return configuredExecutionProvider{}, errors.New("read Orchard trusted certificate file")
		}
	}
	provider, err := dependencies.newOrchardProvider(orchardprovider.Config{
		ControllerURL: configured.Orchard.ControllerURL, ServiceAccount: configured.Orchard.ServiceAccount, Token: strings.TrimSpace(string(token)), VMSpec: spec, TrustedCertificate: certificate,
	})
	if err != nil {
		return configuredExecutionProvider{}, fmt.Errorf("construct Orchard provider: %w", err)
	}
	wrapped := runtimeservice.WithOperationDeadlines(provider, runtimeservice.ProviderName(config.ProviderKindOrchard), observeProviderActivity(dependencies.providerObserver, ""))
	return configuredExecutionProvider{Launcher: wrapped, Inventory: wrapped}, nil
}
