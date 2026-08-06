package daemon

import (
	"context"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

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

func mixedConfiguredProviders() (
	map[core.RunnerPool]execution.ExecutionProvider,
	execution.AggregateRunnerInventory,
	error,
) {
	configured := config.Config{RunnerPools: []config.RunnerPoolConfig{
		{Name: "docker", Docker: &config.DockerConfig{ComposeFile: "compose.yml", Service: "runner"}},
		{Name: "libvirt", Libvirt: &config.LibvirtConfig{
			ConnectionURI: "qemu:///system", DomainXMLTemplate: "domain.xml",
		}},
	}}
	return buildConfiguredProviders(context.Background(), configured, executionDependencies{
		dockerCommands: dockerCommandsFake{
			services: "runner\n",
			runners:  `{"ID":"old-docker","State":"running","Labels":"runnerfoundry.scale-set=libvirt,runnerfoundry.runner=runnerfoundry-00000000-0000-4000-8000-000000000001"}`,
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
		dockerCommands: dockerCommandsFake{
			runners: `{"ID":"old-docker","State":"running","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=old"}`,
		},
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

func TestProductionProviderShellAcceptsEmptyConfiguration(t *testing.T) {
	providers, inventory, err := buildProductionProviders(context.Background(), config.Config{})
	if err != nil || len(providers) != 0 {
		t.Fatalf("empty production providers = (%d, %v)", len(providers), err)
	}
	instances, inspectErr := inventory.Inspect(context.Background())
	if inspectErr != nil || len(instances) != 0 {
		t.Fatalf("empty production inventory = (%#v, %v)", instances, inspectErr)
	}
}
