package execution

import (
	"context"
	"errors"
	"testing"

	providerdocker "github.com/meza/RunnerFoundry/internal/provider/docker"
)

func TestDockerComposeStartTranslatesSourceNeutralRequest(t *testing.T) {
	transport := &recordingDockerTransport{}
	provider := NewDockerComposeProvider(transport, providerdocker.ComposeConfig{
		ComposeFile: "compose.yaml",
		Service:     "runner",
	}, "local")

	err := provider.Start(context.Background(), StartRequest{
		RunnerPool: "linux",
		RunnerName: "linux-1",
		Bootstrap:  NewBootstrap([]byte("opaque-bootstrap")),
	})
	if err != nil {
		t.Fatalf("start Docker Compose runner: %v", err)
	}
	expected := providerdocker.StartRequest{
		RunnerPoolName: "linux",
		RunnerName:     "linux-1",
		JITConfig:      "opaque-bootstrap",
	}
	if transport.startRequest != expected {
		t.Fatalf("start request = %#v, want %#v", transport.startRequest, expected)
	}
}

func TestDockerComposeInspectReturnsOwnedRunnerState(t *testing.T) {
	transport := &recordingDockerTransport{listOutput: `[
{"ID":"container-1","State":"running","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=linux-1"},
{"ID":"container-2","State":"dead","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=linux-2"}
]`}
	provider := NewDockerComposeProvider(transport, providerdocker.ComposeConfig{}, "local")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect Docker Compose runners: %v", err)
	}
	expected := []RunnerInstance{
		{ID: "container-1", RunnerName: "linux-1", RunnerPool: "linux", State: RunnerStateRunning},
		{ID: "container-2", RunnerName: "linux-2", RunnerPool: "linux", State: RunnerStateCompleted},
	}
	if len(instances) != len(expected) {
		t.Fatalf("instances = %#v, want %#v", instances, expected)
	}
	for index := range expected {
		if instances[index] != expected[index] {
			t.Fatalf("instance %d = %#v, want %#v", index, instances[index], expected[index])
		}
	}
}

func TestDockerComposeDestroyTranslatesRunnerIdentityAndState(t *testing.T) {
	transport := &recordingDockerTransport{}
	provider := NewDockerComposeProvider(transport, providerdocker.ComposeConfig{}, "local")

	err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "container-1",
		RunnerName: "linux-1",
		State:      RunnerStateCompleted,
	})
	if err != nil {
		t.Fatalf("destroy Docker Compose runner: %v", err)
	}
	if transport.removedID != "container-1" {
		t.Fatalf("removed ID = %q, want container-1", transport.removedID)
	}
}

func TestDockerComposeValidateChecksConfiguredService(t *testing.T) {
	transport := &recordingDockerTransport{}
	provider := NewDockerComposeProvider(transport, providerdocker.ComposeConfig{
		ComposeFile: "compose.yaml",
		Service:     "runner",
	}, "local")

	if err := provider.Validate(context.Background()); err != nil {
		t.Fatalf("validate Docker Compose provider: %v", err)
	}
	if transport.validatedConfig != (providerdocker.ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"}) {
		t.Fatalf("validated config = %#v", transport.validatedConfig)
	}
}

func TestDockerComposeValidatePreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("validate failed")
	provider := NewDockerComposeProvider(
		&recordingDockerTransport{validateErr: expectedErr},
		providerdocker.ComposeConfig{},
		"local",
	)

	err := provider.Validate(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("validate error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestDockerComposeInspectPreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("inspect failed")
	provider := NewDockerComposeProvider(
		&recordingDockerTransport{listErr: expectedErr},
		providerdocker.ComposeConfig{},
		"local",
	)

	_, err := provider.Inspect(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("inspect error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestDockerComposeStartPreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("start failed")
	provider := NewDockerComposeProvider(
		&recordingDockerTransport{startErr: expectedErr},
		providerdocker.ComposeConfig{},
		"local",
	)

	err := provider.Start(context.Background(), StartRequest{})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("start error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestDockerComposeDestroyPreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("destroy failed")
	provider := NewDockerComposeProvider(
		&recordingDockerTransport{removeErr: expectedErr},
		providerdocker.ComposeConfig{},
		"local",
	)

	err := provider.Destroy(context.Background(), RunnerInstance{State: RunnerStateRunning})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("destroy error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestDockerComposeEndpointOperationsAreExplicit(t *testing.T) {
	provider := NewDockerComposeProvider(&recordingDockerTransport{}, providerdockerTestConfig(), "member-a")
	endpointID := InventoryEndpointID("docker-compose:member-a?compose=compose.yaml&service=runner")
	if provider.EndpointID() != endpointID || !provider.KnowsEndpoint(endpointID) ||
		provider.KnowsEndpoint("docker-compose:member-b?compose=compose.yaml&service=runner") {
		t.Fatalf("endpoint identity = %q", provider.EndpointID())
	}
	if !provider.CanInspectEndpointForMember(endpointID, "member-a") ||
		provider.CanInspectEndpointForMember(endpointID, "member-b") {
		t.Fatal("Docker endpoint inspection must remain local to its owning member")
	}
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if _, err := provider.InspectEndpoint(context.Background(), endpointID); err != nil {
		t.Fatalf("inspect endpoint: %v", err)
	}
	if _, err := provider.InspectEndpoint(context.Background(), "docker-compose:member-b?compose=compose.yaml&service=runner"); err == nil {
		t.Fatal("unknown endpoint accepted")
	}
}

func TestDockerComposeEndpointDistinguishesMechanismsOnOneMember(t *testing.T) {
	first := NewDockerComposeProvider(&recordingDockerTransport{}, providerdockerTestConfig(), " MEMBER-A ")
	secondConfig := providerdockerTestConfig()
	secondConfig.Service = "gpu-runner"
	second := NewDockerComposeProvider(&recordingDockerTransport{}, secondConfig, "member-a")
	if first.EndpointID() == second.EndpointID() {
		t.Fatalf("distinct Docker mechanisms share endpoint %q", first.EndpointID())
	}
}

type recordingDockerTransport struct {
	startRequest    providerdocker.StartRequest
	startErr        error
	listOutput      string
	listErr         error
	listCalls       int
	removedID       string
	removeErr       error
	validatedConfig providerdocker.ComposeConfig
	validateErr     error
}

func (transport *recordingDockerTransport) StartComposeRunner(
	_ context.Context,
	_ providerdocker.ComposeConfig,
	request providerdocker.StartRequest,
) error {
	transport.startRequest = request
	return transport.startErr
}

func (transport *recordingDockerTransport) ListComposeRunners(context.Context, string) (string, error) {
	transport.listCalls++
	return transport.listOutput, transport.listErr
}

func (transport *recordingDockerTransport) RemoveComposeRunner(_ context.Context, containerID string) error {
	transport.removedID = containerID
	return transport.removeErr
}

func (transport *recordingDockerTransport) ListComposeServices(
	_ context.Context,
	config providerdocker.ComposeConfig,
) (string, error) {
	transport.validatedConfig = config
	return "runner\n", transport.validateErr
}
