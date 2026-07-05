package daemon

import (
	"context"
	"fmt"

	"github.com/meza/github-scaler/internal/config"
	dockerprovider "github.com/meza/github-scaler/internal/provider/docker"
	libvirtprovider "github.com/meza/github-scaler/internal/provider/libvirt"
	"github.com/meza/github-scaler/internal/scaler"
)

func buildReconciler(ctx context.Context, scaleSetConfig config.ScaleSetConfig, client scaleSetClient, scaleSetID int) (scaler.Reconciler, error) {
	return buildReconcilerWithRuntime(ctx, scaleSetConfig, client, scaleSetID, productionProviderRuntime())
}

type providerRuntime struct {
	DockerCommandRunner  dockerprovider.CommandRunner
	LibvirtCommandRunner libvirtprovider.CommandRunner
}

func productionProviderRuntime() providerRuntime {
	return providerRuntime{
		DockerCommandRunner:  dockerprovider.ExecCommandRunner{},
		LibvirtCommandRunner: libvirtprovider.ExecCommandRunner{},
	}
}

func buildReconcilerWithRuntime(
	ctx context.Context,
	scaleSetConfig config.ScaleSetConfig,
	client scaleSetClient,
	scaleSetID int,
	providers providerRuntime,
) (scaler.Reconciler, error) {
	if scaleSetConfig.Docker != nil {
		return buildDockerReconciler(ctx, scaleSetConfig, client, scaleSetID, providers.DockerCommandRunner)
	}
	if scaleSetConfig.Libvirt != nil {
		return buildLibvirtReconciler(ctx, scaleSetConfig, client, scaleSetID, providers.LibvirtCommandRunner)
	}
	return scaler.Reconciler{}, fmt.Errorf("scale set %q has no provider", scaleSetConfig.Name)
}

func buildDockerReconciler(
	ctx context.Context,
	scaleSetConfig config.ScaleSetConfig,
	client scaleSetClient,
	scaleSetID int,
	commandRunner dockerprovider.CommandRunner,
) (scaler.Reconciler, error) {
	provider := dockerProviderAdapter{
		Provider: dockerprovider.ComposeProvider{
			CommandRunner: commandRunner,
			Config: dockerprovider.ComposeConfig{
				ComposeFile: scaleSetConfig.Docker.ComposeFile,
				Service:     scaleSetConfig.Docker.Service,
			},
		},
	}
	return validatedReconciler(ctx, scaleSetConfig, provider, provider.Provider.Validate, "docker", client, scaleSetID)
}

func buildLibvirtReconciler(
	ctx context.Context,
	scaleSetConfig config.ScaleSetConfig,
	client scaleSetClient,
	scaleSetID int,
	commandRunner libvirtprovider.CommandRunner,
) (scaler.Reconciler, error) {
	provider := libvirtProviderAdapter{
		Provider: libvirtprovider.Provider{
			CommandRunner: commandRunner,
			Config: libvirtprovider.Config{
				ConnectionURI:     scaleSetConfig.Libvirt.ConnectionURI,
				DomainXMLTemplate: scaleSetConfig.Libvirt.DomainXMLTemplate,
			},
		},
	}
	return validatedReconciler(ctx, scaleSetConfig, provider, provider.Provider.Validate, "libvirt", client, scaleSetID)
}

func newReconciler(scaleSetConfig config.ScaleSetConfig, provider scaler.Provider, client scaleSetClient, scaleSetID int) scaler.Reconciler {
	return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name:       scaleSetConfig.Name,
		MaxRunners: scaleSetConfig.MaxRunners,
		Provider:   provider,
		JITConfig:  scalesetJITGenerator{Client: client, ScaleSetID: scaleSetID},
	}}
}

func validatedReconciler(
	ctx context.Context,
	scaleSetConfig config.ScaleSetConfig,
	provider scaler.Provider,
	validate func(ctx context.Context) error,
	providerName string,
	client scaleSetClient,
	scaleSetID int,
) (scaler.Reconciler, error) {
	if err := validate(ctx); err != nil {
		return scaler.Reconciler{}, fmt.Errorf("validate %s provider for %q: %w", providerName, scaleSetConfig.Name, err)
	}
	return newReconciler(scaleSetConfig, provider, client, scaleSetID), nil
}

type dockerProviderAdapter struct {
	Provider dockerprovider.ComposeProvider
}

func (adapter dockerProviderAdapter) List(ctx context.Context, scaleSetName string) ([]scaler.RunnerInstance, error) {
	instances, err := adapter.Provider.List(ctx, scaleSetName)
	if err != nil {
		return nil, fmt.Errorf("list docker provider instances: %w", err)
	}
	return convertDockerInstances(instances), nil
}

func (adapter dockerProviderAdapter) Start(ctx context.Context, request scaler.RunnerStartRequest) error {
	if err := adapter.Provider.Start(ctx, dockerprovider.StartRequest{
		ScaleSetName: request.ScaleSetName,
		RunnerName:   request.RunnerName,
		JITConfig:    request.JITConfig,
	}); err != nil {
		return fmt.Errorf("start docker provider runner: %w", err)
	}
	return nil
}

func (adapter dockerProviderAdapter) Destroy(ctx context.Context, instance scaler.RunnerInstance) error {
	if err := adapter.Provider.Destroy(ctx, dockerprovider.RunnerInstance{
		ID:         instance.ID,
		RunnerName: instance.RunnerName,
		State:      dockerprovider.RunnerStateCompleted,
	}); err != nil {
		return fmt.Errorf("destroy docker provider runner: %w", err)
	}
	return nil
}

func convertDockerInstances(instances []dockerprovider.RunnerInstance) []scaler.RunnerInstance {
	convertedInstances := make([]scaler.RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		convertedInstances = append(convertedInstances, scaler.RunnerInstance{
			ID:         instance.ID,
			RunnerName: instance.RunnerName,
			State:      dockerRunnerState(instance),
		})
	}
	return convertedInstances
}

func dockerRunnerState(instance dockerprovider.RunnerInstance) scaler.RunnerState {
	if instance.State == dockerprovider.RunnerStateRunning {
		return scaler.RunnerStateRunning
	}
	return scaler.RunnerStateCompleted
}

type libvirtProviderAdapter struct {
	Provider libvirtprovider.Provider
}

func (adapter libvirtProviderAdapter) List(ctx context.Context, scaleSetName string) ([]scaler.RunnerInstance, error) {
	instances, err := adapter.Provider.List(ctx, scaleSetName)
	if err != nil {
		return nil, fmt.Errorf("list libvirt provider instances: %w", err)
	}
	return convertLibvirtInstances(instances), nil
}

func (adapter libvirtProviderAdapter) Start(ctx context.Context, request scaler.RunnerStartRequest) error {
	if err := adapter.Provider.Start(ctx, libvirtprovider.StartRequest{
		ScaleSetName: request.ScaleSetName,
		RunnerName:   request.RunnerName,
		JITConfig:    request.JITConfig,
	}); err != nil {
		return fmt.Errorf("start libvirt provider runner: %w", err)
	}
	return nil
}

func (adapter libvirtProviderAdapter) Destroy(ctx context.Context, instance scaler.RunnerInstance) error {
	if err := adapter.Provider.Destroy(ctx, libvirtprovider.RunnerInstance{
		ID:         instance.ID,
		RunnerName: instance.RunnerName,
		State:      libvirtprovider.RunnerStateCompleted,
	}); err != nil {
		return fmt.Errorf("destroy libvirt provider runner: %w", err)
	}
	return nil
}

func convertLibvirtInstances(instances []libvirtprovider.RunnerInstance) []scaler.RunnerInstance {
	convertedInstances := make([]scaler.RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		convertedInstances = append(convertedInstances, scaler.RunnerInstance{
			ID:         instance.ID,
			RunnerName: instance.RunnerName,
			State:      libvirtRunnerState(instance),
		})
	}
	return convertedInstances
}

func libvirtRunnerState(instance libvirtprovider.RunnerInstance) scaler.RunnerState {
	if instance.State == libvirtprovider.RunnerStateRunning {
		return scaler.RunnerStateRunning
	}
	return scaler.RunnerStateCompleted
}
