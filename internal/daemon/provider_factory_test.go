package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	libvirtprovider "github.com/stateshifters/runnerfoundry/internal/provider/libvirt"
)

func TestBuildExecutionProviderValidatesConfiguredDockerComposeService(t *testing.T) {
	var receivedConfig dockerprovider.ComposeConfig
	dependencies := executionDependencies{
		newDockerLauncher: func(config dockerprovider.ComposeConfig, _ core.MemberID) execution.RunnerLauncher {
			receivedConfig = config
			return providerFactoryLauncherFake{endpointID: "docker:member-a"}
		},
	}
	provider, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{
		Name: "linux",
		Docker: &config.DockerConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}, dependencies)
	if err != nil || provider.Launcher == nil || provider.Inventory != nil ||
		receivedConfig != (dockerprovider.ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"}) {
		t.Fatalf("build provider = (%T, %v)", provider, err)
	}
}

func TestBuildExecutionProviderBuildsLibvirtProvider(t *testing.T) {
	t.Parallel()

	configured := config.RunnerPoolConfig{Name: "linux", Libvirt: &config.LibvirtConfig{
		ConnectionURI: "qemu:///system", DomainXMLTemplate: "domain.xml",
	}}
	if _, err := buildExecutionProvider(context.Background(), configured, executionDependencies{}); err == nil {
		t.Fatal("missing template reader was accepted")
	}
	wantErr := errors.New("read failed")
	if _, err := buildExecutionProvider(context.Background(), configured, executionDependencies{
		readTemplate: func(string) ([]byte, error) { return nil, wantErr },
	}); !errors.Is(err, wantErr) {
		t.Fatalf("template error = %v", err)
	}
	provider, err := buildExecutionProvider(context.Background(), configured, executionDependencies{
		readTemplate: func(string) ([]byte, error) {
			return []byte(`<domain type="kvm"><name>template</name></domain>`), nil
		},
		libvirtCommands: libvirtCommandsFake{},
	})
	if err != nil || provider.Launcher == nil || provider.Inventory == nil {
		t.Fatalf("build libvirt provider = (%T, %T), %v", provider.Launcher, provider.Inventory, err)
	}
}

func TestBuildExecutionProviderReportsValidationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("validation failed")
	_, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{
		Name: "linux", Docker: &config.DockerConfig{ComposeFile: "compose.yml", Service: "runner"},
	}, executionDependencies{newDockerLauncher: func(dockerprovider.ComposeConfig, core.MemberID) execution.RunnerLauncher {
		return providerFactoryLauncherFake{err: wantErr}
	}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("validation error = %v", err)
	}
}

func TestBuildExecutionProviderRejectsMissingProvider(t *testing.T) {
	if _, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{Name: "linux"}, executionDependencies{}); err == nil {
		t.Fatal("provider-less Runner Pool was accepted")
	}
}

func TestDockerCompositionRequiresLauncherAndInventoryFactories(t *testing.T) {
	configured := config.RunnerPoolConfig{Name: "linux", Docker: &config.DockerConfig{Service: "runner"}}
	if _, err := buildExecutionProvider(context.Background(), configured, executionDependencies{}); err == nil {
		t.Fatal("missing Docker launcher factory was accepted")
	}
	if _, _, err := buildProductionProvidersWithDependencies(context.Background(), config.Config{
		RunnerPools: []config.RunnerPoolConfig{configured},
	}, "member-a", productionDependencies{}); err == nil {
		t.Fatal("missing Docker inventory factory was accepted")
	}
}

type providerFactoryLauncherFake struct {
	endpointID execution.InventoryEndpointID
	err        error
}

func (provider providerFactoryLauncherFake) EndpointID() execution.InventoryEndpointID {
	return provider.endpointID
}
func (provider providerFactoryLauncherFake) Validate(context.Context) error { return provider.err }
func (providerFactoryLauncherFake) Start(context.Context, execution.StartRequest) error {
	return nil
}

type libvirtCommandsFake struct{}

func (libvirtCommandsFake) CreateDomain(context.Context, string, string) error  { return nil }
func (libvirtCommandsFake) ListDomains(context.Context, string) (string, error) { return "", nil }
func (libvirtCommandsFake) ReadDomainXML(context.Context, string, string) (string, error) {
	return "", nil
}
func (libvirtCommandsFake) ReadDomainState(context.Context, string, string) (string, error) {
	return "", nil
}
func (libvirtCommandsFake) DestroyDomain(context.Context, string, string) error { return nil }
func (libvirtCommandsFake) ReadConnectionURI(context.Context, string) (string, error) {
	return "qemu:///system", nil
}

var _ libvirtprovider.CommandRunner = libvirtCommandsFake{}
