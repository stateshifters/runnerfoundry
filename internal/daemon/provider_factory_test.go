package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/config"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	libvirtprovider "github.com/stateshifters/runnerfoundry/internal/provider/libvirt"
)

func TestBuildExecutionProviderValidatesConfiguredDockerComposeService(t *testing.T) {
	dependencies := executionDependencies{
		dockerCommands: dockerCommandsFake{services: "runner\n"},
	}
	provider, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{
		Name: "linux",
		Docker: &config.DockerConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}, dependencies)
	if err != nil || provider.ExecutionProvider == nil {
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
	if err != nil || provider.ExecutionProvider == nil {
		t.Fatalf("build libvirt provider = %T, %v", provider.ExecutionProvider, err)
	}
}

func TestBuildExecutionProviderReportsValidationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("validation failed")
	_, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{
		Name: "linux", Docker: &config.DockerConfig{ComposeFile: "compose.yml", Service: "runner"},
	}, executionDependencies{dockerCommands: dockerCommandsErrorFake{err: wantErr}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("validation error = %v", err)
	}
}

func TestBuildExecutionProviderRejectsMissingProvider(t *testing.T) {
	if _, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{Name: "linux"}, executionDependencies{}); err == nil {
		t.Fatal("provider-less Runner Pool was accepted")
	}
}

type dockerCommandsFake struct {
	services string
	runners  string
}

type dockerCommandsErrorFake struct{ err error }

func (fake dockerCommandsErrorFake) StartComposeRunner(context.Context, dockerprovider.ComposeConfig, dockerprovider.StartRequest) error {
	return nil
}
func (fake dockerCommandsErrorFake) ListComposeRunners(context.Context, string) (string, error) {
	return "", nil
}
func (fake dockerCommandsErrorFake) RemoveComposeRunner(context.Context, string) error { return nil }
func (fake dockerCommandsErrorFake) ListComposeServices(context.Context, dockerprovider.ComposeConfig) (string, error) {
	return "", fake.err
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

func (dockerCommandsFake) StartComposeRunner(context.Context, dockerprovider.ComposeConfig, dockerprovider.StartRequest) error {
	return nil
}

func (commands dockerCommandsFake) ListComposeRunners(context.Context, string) (string, error) {
	return commands.runners, nil
}

func (dockerCommandsFake) RemoveComposeRunner(context.Context, string) error {
	return nil
}

func (commands dockerCommandsFake) ListComposeServices(context.Context, dockerprovider.ComposeConfig) (string, error) {
	return commands.services, nil
}
