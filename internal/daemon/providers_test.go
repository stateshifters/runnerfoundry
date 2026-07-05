package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/meza/github-scaler/internal/config"
	dockerprovider "github.com/meza/github-scaler/internal/provider/docker"
	libvirtprovider "github.com/meza/github-scaler/internal/provider/libvirt"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestBuildReconcilerRejectsMissingProvider(t *testing.T) {
	if _, err := buildReconcilerWithRuntime(context.Background(), "https://github.com/meza/repo-a", config.ScaleSetConfig{Name: "linux"}, &fakeScaleSetClient{}, 1, providerRuntime{}); err == nil {
		t.Fatal("expected missing provider error")
	}
}

func TestBuildDockerReconcilerValidatesProvider(t *testing.T) {
	reconciler, err := buildReconcilerWithRuntime(context.Background(), "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:       "linux",
		MaxRunners: 2,
		Docker:     &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	}, &fakeScaleSetClient{}, 5, providerRuntime{DockerCommandRunner: dockerRunnerStub{output: "runner\n"}})
	if err != nil {
		t.Fatalf("expected docker reconciler, got %v", err)
	}
	if reconciler.ScaleSet.Name != "github-com-meza-repo-a-linux" || reconciler.ScaleSet.MaxRunners != 2 {
		t.Fatalf("unexpected reconciler: %#v", reconciler)
	}
}

func TestBuildDockerReconcilerSurfacesValidationFailure(t *testing.T) {
	_, err := buildReconcilerWithRuntime(context.Background(), "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:   "linux",
		Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	}, &fakeScaleSetClient{}, 5, providerRuntime{DockerCommandRunner: dockerRunnerStub{output: "other\n"}})
	if err == nil || !strings.Contains(err.Error(), "validate docker provider") {
		t.Fatalf("expected docker validation error, got %v", err)
	}
}

func TestBuildLibvirtReconcilerValidatesProvider(t *testing.T) {
	reconciler, err := buildReconcilerWithRuntime(context.Background(), "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name:       "linux-vm",
		MaxRunners: 1,
		Libvirt: &config.LibvirtConfig{
			ConnectionURI:     "qemu:///system",
			DomainXMLTemplate: writeFile(t, "domain.xml", `<domain type="kvm"><name>template</name></domain>`),
		},
	}, &fakeScaleSetClient{}, 5, providerRuntime{LibvirtCommandRunner: &libvirtRunnerStub{outputs: []string{"qemu:///system"}}})
	if err != nil {
		t.Fatalf("expected libvirt reconciler, got %v", err)
	}
	if reconciler.ScaleSet.Name != "github-com-meza-repo-a-linux-vm" || reconciler.ScaleSet.MaxRunners != 1 {
		t.Fatalf("unexpected reconciler: %#v", reconciler)
	}
}

func TestBuildLibvirtReconcilerSurfacesValidationFailure(t *testing.T) {
	_, err := buildReconcilerWithRuntime(context.Background(), "https://github.com/meza/repo-a", config.ScaleSetConfig{
		Name: "linux-vm",
		Libvirt: &config.LibvirtConfig{
			ConnectionURI:     "qemu:///system",
			DomainXMLTemplate: filepath.Join(t.TempDir(), "missing.xml"),
		},
	}, &fakeScaleSetClient{}, 5, providerRuntime{LibvirtCommandRunner: &libvirtRunnerStub{}})
	if err == nil || !strings.Contains(err.Error(), "validate libvirt provider") {
		t.Fatalf("expected libvirt validation error, got %v", err)
	}
}

func TestBuildReconcilerUsesProductionRuntime(t *testing.T) {
	if _, err := buildReconciler(context.Background(), "https://github.com/meza/repo-a", config.ScaleSetConfig{Name: "linux"}, &fakeScaleSetClient{}, 1); err == nil {
		t.Fatal("expected missing provider error")
	}
}

func TestDockerProviderAdapterConvertsProviderBehavior(t *testing.T) {
	commandRunner := &dockerRecordingRunner{outputs: []string{
		`{"ID":"1","State":"running","Labels":"github-scaler.scale-set=linux,github-scaler.runner=linux-1"}`,
		"",
		"",
	}}
	adapter := dockerProviderAdapter{Provider: dockerprovider.ComposeProvider{
		CommandRunner: commandRunner,
		Config:        dockerprovider.ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"},
	}}
	instances, err := adapter.List(context.Background(), "linux")
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	expectedInstances := []scaler.RunnerInstance{{ID: "1", RunnerName: "linux-1", State: scaler.RunnerStateRunning}}
	if !reflect.DeepEqual(instances, expectedInstances) {
		t.Fatalf("unexpected instances\nwant %#v\ngot  %#v", expectedInstances, instances)
	}
	if err := adapter.Start(context.Background(), scaler.RunnerStartRequest{ScaleSetName: "linux", RunnerName: "linux-2", JITConfig: "jit"}); err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	if err := adapter.Destroy(context.Background(), scaler.RunnerInstance{ID: "1", RunnerName: "linux-1"}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
}

func TestDockerProviderAdapterSurfacesProviderErrors(t *testing.T) {
	adapter := dockerProviderAdapter{Provider: dockerprovider.ComposeProvider{CommandRunner: dockerFailingRunner{}}}
	if _, err := adapter.List(context.Background(), "linux"); err == nil {
		t.Fatal("expected list error")
	}
	if err := adapter.Start(context.Background(), scaler.RunnerStartRequest{}); err == nil {
		t.Fatal("expected start error")
	}
	if err := adapter.Destroy(context.Background(), scaler.RunnerInstance{}); err == nil {
		t.Fatal("expected destroy error")
	}
}

func TestDockerRunnerStateMapsCompletedStates(t *testing.T) {
	if dockerRunnerState(dockerprovider.RunnerInstance{State: dockerprovider.RunnerStateCompleted}) != scaler.RunnerStateCompleted {
		t.Fatal("expected completed docker state")
	}
}

func TestLibvirtProviderAdapterConvertsProviderBehavior(t *testing.T) {
	commandRunner := &libvirtRunnerStub{outputs: []string{
		"linux-vm-1\n",
		`<domain><metadata><github-scaler.scale-set>linux-vm</github-scaler.scale-set><github-scaler.runner>linux-vm-1</github-scaler.runner></metadata></domain>`,
		"running",
		"",
		"",
	}}
	adapter := libvirtProviderAdapter{Provider: libvirtprovider.Provider{
		CommandRunner: commandRunner,
		Config:        libvirtprovider.Config{ConnectionURI: "qemu:///system", DomainXMLTemplate: writeFile(t, "domain.xml", `<domain type="kvm"><name>template</name></domain>`)},
	}}
	instances, err := adapter.List(context.Background(), "linux-vm")
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	expectedInstances := []scaler.RunnerInstance{{ID: "linux-vm-1", RunnerName: "linux-vm-1", State: scaler.RunnerStateRunning}}
	if !reflect.DeepEqual(instances, expectedInstances) {
		t.Fatalf("unexpected instances\nwant %#v\ngot  %#v", expectedInstances, instances)
	}
	if err := adapter.Start(context.Background(), scaler.RunnerStartRequest{ScaleSetName: "linux-vm", RunnerName: "linux-vm-2", JITConfig: "jit"}); err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	if err := adapter.Destroy(context.Background(), scaler.RunnerInstance{ID: "linux-vm-1", RunnerName: "linux-vm-1"}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
}

func TestLibvirtProviderAdapterSurfacesProviderErrors(t *testing.T) {
	adapter := libvirtProviderAdapter{Provider: libvirtprovider.Provider{CommandRunner: libvirtFailingRunner{}}}
	if _, err := adapter.List(context.Background(), "linux"); err == nil {
		t.Fatal("expected list error")
	}
	if err := adapter.Start(context.Background(), scaler.RunnerStartRequest{}); err == nil {
		t.Fatal("expected start error")
	}
	if err := adapter.Destroy(context.Background(), scaler.RunnerInstance{}); err == nil {
		t.Fatal("expected destroy error")
	}
}

func TestLibvirtRunnerStateMapsCompletedStates(t *testing.T) {
	if libvirtRunnerState(libvirtprovider.RunnerInstance{State: libvirtprovider.RunnerStateCompleted}) != scaler.RunnerStateCompleted {
		t.Fatal("expected completed libvirt state")
	}
}

type dockerRunnerStub struct {
	output string
	err    error
}

func (runner dockerRunnerStub) Run(ctx context.Context, command dockerprovider.Command) (string, error) {
	if runner.err != nil {
		return "", runner.err
	}
	return runner.output, nil
}

type dockerRecordingRunner struct {
	commands []dockerprovider.Command
	outputs  []string
}

func (runner *dockerRecordingRunner) Run(ctx context.Context, command dockerprovider.Command) (string, error) {
	runner.commands = append(runner.commands, command)
	if len(runner.outputs) == 0 {
		return "", nil
	}
	output := runner.outputs[0]
	runner.outputs = runner.outputs[1:]
	return output, nil
}

type dockerFailingRunner struct{}

func (runner dockerFailingRunner) Run(ctx context.Context, command dockerprovider.Command) (string, error) {
	return "", errors.New("docker failed")
}

type libvirtRunnerStub struct {
	commands []libvirtprovider.Command
	outputs  []string
}

func (runner *libvirtRunnerStub) Run(ctx context.Context, command libvirtprovider.Command) (string, error) {
	runner.commands = append(runner.commands, command)
	if len(runner.outputs) == 0 {
		return "", nil
	}
	output := runner.outputs[0]
	runner.outputs = runner.outputs[1:]
	return output, nil
}

type libvirtFailingRunner struct{}

func (runner libvirtFailingRunner) Run(ctx context.Context, command libvirtprovider.Command) (string, error) {
	return "", errors.New("libvirt failed")
}
