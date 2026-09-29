package daemon

import (
	"context"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	runtimeservice "github.com/stateshifters/runnerfoundry/internal/runtime"
)

type observedProviderActivity struct {
	activity       runtimeservice.ProviderActivity
	composeService string
}

func TestProductionProviderComposition(t *testing.T) {
	providers, _, err := mixedConfiguredProviders()
	if err != nil || len(providers) != 2 {
		t.Fatalf("providers = (%d, %v)", len(providers), err)
	}
	if providers[core.RunnerPool("docker")] == nil {
		t.Fatal("Docker provider is missing")
	}
}

func TestProductionProviderCompositionRejectsMissingProvider(t *testing.T) {
	if _, _, err := buildConfiguredProviders(context.Background(), config.Config{
		RunnerPools: []config.RunnerPoolConfig{{Name: "bad"}},
	}, executionDependencies{}); err == nil {
		t.Fatal("invalid provider composition succeeded")
	}
}

func TestProductionInventoryCombinesActiveProviderBackends(t *testing.T) {
	_, inventory, err := mixedConfiguredProviders()
	if err != nil {
		t.Fatalf("build mixed providers: %v", err)
	}
	instances, inspectErr := inventory.Inspect(context.Background())
	if inspectErr != nil || len(instances) != 1 || instances[0].ID != "old-docker" {
		t.Fatalf("provider-agnostic inventory = (%#v, %v)", instances, inspectErr)
	}
}

func TestDockerPoolsShareEndpointInventoryWithoutComposeServiceMetadata(t *testing.T) {
	var activities []observedProviderActivity
	var launcherConfigs []dockerprovider.ComposeConfig
	inventoryBuilds := 0
	providers, inventory, err := buildProductionProvidersWithDependencies(context.Background(), twoDockerPoolConfiguration(), "member-a", productionDependencies{
		newDockerLauncher: func(configured dockerprovider.ComposeConfig, member core.MemberID) execution.RunnerLauncher {
			if member != "member-a" {
				t.Fatalf("launcher member = %q, want member-a", member)
			}
			launcherConfigs = append(launcherConfigs, configured)
			return providerFactoryLauncherFake{endpointID: "docker:member-a"}
		},
		newDockerInventory: func(member core.MemberID) execution.RunnerInventory {
			if member != "member-a" {
				t.Fatalf("inventory member = %q, want member-a", member)
			}
			inventoryBuilds++
			return &providerFactoryInventoryFake{endpointID: "docker:member-a"}
		},
		providerObserver: func(activity runtimeservice.ProviderActivity, composeService string) {
			activities = append(activities, observedProviderActivity{activity: activity, composeService: composeService})
		},
	})
	if err != nil {
		t.Fatalf("build providers: %v", err)
	}
	if inventoryBuilds != 1 {
		t.Fatalf("Docker inventory builds = %d, want 1", inventoryBuilds)
	}
	assertDockerFactoryConfigurations(t, launcherConfigs)
	assertDockerLaunchersUseConfiguredServices(t, providers, &activities)
	assertDockerInventoryHasNoComposeService(t, inventory, &activities)
}

func assertDockerFactoryConfigurations(t *testing.T, configurations []dockerprovider.ComposeConfig) {
	t.Helper()
	if len(configurations) != 2 || configurations[0] != (dockerprovider.ComposeConfig{ComposeFile: "a.yml", Service: "runner-a"}) ||
		configurations[1] != (dockerprovider.ComposeConfig{ComposeFile: "b.yml", Service: "runner-b"}) {
		t.Fatalf("launcher configurations = %#v, want the two configured Compose services", configurations)
	}
}

func twoDockerPoolConfiguration() config.Config {
	return config.Config{
		RunnerPools: []config.RunnerPoolConfig{
			{Name: "linux-a", Docker: &config.DockerConfig{ComposeFile: "a.yml", Service: "runner-a"}},
			{Name: "linux-b", Docker: &config.DockerConfig{ComposeFile: "b.yml", Service: "runner-b"}},
		},
	}
}

func assertDockerLaunchersUseConfiguredServices(
	t *testing.T,
	providers map[core.RunnerPool]execution.RunnerLauncher,
	activities *[]observedProviderActivity,
) {
	t.Helper()
	if len(providers) != 2 {
		t.Fatalf("providers = %d, want 2", len(providers))
	}
	assertDockerLauncherUsesService(t, providers["linux-a"], "linux-a", "runner-a", activities)
	assertDockerLauncherUsesService(t, providers["linux-b"], "linux-b", "runner-b", activities)
}

func assertDockerLauncherUsesService(
	t *testing.T,
	launcher execution.RunnerLauncher,
	runnerPool core.RunnerPool,
	expectedService string,
	activities *[]observedProviderActivity,
) {
	t.Helper()
	if err := launcher.Start(context.Background(), execution.StartRequest{
		RunnerPool: runnerPool,
		EndpointID: launcher.EndpointID(),
	}); err != nil {
		t.Fatalf("start %s: %v", runnerPool, err)
	}
	for _, activity := range *activities {
		if activity.activity.Operation == "start" && activity.activity.RunnerPool == runnerPool &&
			activity.composeService == expectedService {
			return
		}
	}
	t.Fatalf("start activity for %s did not use Compose service %q: %#v", runnerPool, expectedService, *activities)
}

func assertDockerInventoryHasNoComposeService(
	t *testing.T,
	inventory execution.RunnerInventory,
	activities *[]observedProviderActivity,
) {
	t.Helper()
	if _, err := inventory.Inspect(context.Background()); err != nil {
		t.Fatalf("inspect inventory: %v", err)
	}
	for _, activity := range *activities {
		if activity.activity.Operation == "inspect" && activity.composeService != "" {
			t.Fatalf("inventory activity claimed Compose service: %#v", activity)
		}
	}
}

func mixedConfiguredProviders() (
	map[core.RunnerPool]execution.RunnerLauncher,
	runtimeservice.AggregateRunnerInventory,
	error,
) {
	configured := config.Config{RunnerPools: []config.RunnerPoolConfig{
		{Name: "docker", Docker: &config.DockerConfig{ComposeFile: "compose.yml", Service: "runner"}},
		{Name: "libvirt", Libvirt: &config.LibvirtConfig{
			ConnectionURI: "qemu:///system", DomainXMLTemplate: "domain.xml",
		}},
	}}
	return buildConfiguredProviders(context.Background(), configured, executionDependencies{
		newDockerLauncher: func(dockerprovider.ComposeConfig, core.MemberID) execution.RunnerLauncher {
			return providerFactoryLauncherFake{endpointID: "docker:member-a"}
		},
		newDockerInventory: func(core.MemberID) execution.RunnerInventory {
			return &providerFactoryInventoryFake{endpointID: "docker:member-a", instances: []execution.RunnerInstance{{
				ID: "old-docker", RunnerName: "external-runner", RunnerPool: "libvirt", State: execution.RunnerStateRunning,
			}}}
		},
		libvirtCommands: libvirtCommandsFake{},
		readTemplate: func(string) ([]byte, error) {
			return []byte(`<domain type="kvm"><name>template</name></domain>`), nil
		},
	})
}

func TestProductionInventoryContainsOnlyActiveConfigurationEndpoints(t *testing.T) {
	configured := config.Config{RunnerPools: []config.RunnerPoolConfig{{
		Name: "linux",
		Libvirt: &config.LibvirtConfig{
			ConnectionURI: "qemu:///new", DomainXMLTemplate: "domain.xml",
		},
	}}}
	_, inventory, err := buildConfiguredProviders(context.Background(), configured, executionDependencies{
		libvirtCommands: libvirtCommandsFake{},
		readTemplate: func(string) ([]byte, error) {
			return []byte(`<domain type="kvm"><name>template</name></domain>`), nil
		},
	})
	if err != nil {
		t.Fatalf("build current inventory: %v", err)
	}
	instances, err := inventory.Inspect(context.Background())
	if err != nil || len(instances) != 0 {
		t.Fatalf("current inventory = (%#v, %v), want no retired Docker resources", instances, err)
	}
}

type providerFactoryInventoryFake struct {
	endpointID execution.InventoryEndpointID
	instances  []execution.RunnerInstance
}

func (inventory *providerFactoryInventoryFake) EndpointID() execution.InventoryEndpointID {
	return inventory.endpointID
}
func (inventory *providerFactoryInventoryFake) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	return inventory.endpointID == endpointID
}
func (inventory *providerFactoryInventoryFake) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	_ core.MemberID,
) bool {
	return inventory.KnowsEndpoint(endpointID)
}
func (inventory *providerFactoryInventoryFake) Refresh(context.Context) error { return nil }
func (inventory *providerFactoryInventoryFake) Inspect(context.Context) ([]execution.RunnerInstance, error) {
	return append([]execution.RunnerInstance(nil), inventory.instances...), nil
}
func (inventory *providerFactoryInventoryFake) InspectEndpoint(
	ctx context.Context,
	_ execution.InventoryEndpointID,
) ([]execution.RunnerInstance, error) {
	return inventory.Inspect(ctx)
}
func (*providerFactoryInventoryFake) Destroy(context.Context, execution.RunnerInstance) error {
	return nil
}

func TestProviderBuilderAcceptsEmptyConfiguration(t *testing.T) {
	providers, inventory, err := buildProductionProvidersWithDependencies(
		context.Background(), config.Config{}, "", productionDependencies{},
	)
	if err != nil || len(providers) != 0 {
		t.Fatalf("empty production providers = (%d, %v)", len(providers), err)
	}
	instances, inspectErr := inventory.Inspect(context.Background())
	if inspectErr != nil || len(instances) != 0 {
		t.Fatalf("empty production inventory = (%#v, %v)", instances, inspectErr)
	}
}

func TestProductionDockerFactoriesConstructSeparateBoundaries(t *testing.T) {
	dependencies := productionExecutionDependencies(newProductionDependencies(), "member-a")
	launcher := dependencies.newDockerLauncher(dockerprovider.ComposeConfig{Service: "runner"}, "member-a")
	inventory := dependencies.newDockerInventory("member-a")
	if launcher == nil || inventory == nil || launcher.EndpointID() != inventory.EndpointID() {
		t.Fatalf("Docker boundaries = (%T, %T), endpoints = (%q, %q)",
			launcher, inventory, launcher.EndpointID(), inventory.EndpointID())
	}
}
