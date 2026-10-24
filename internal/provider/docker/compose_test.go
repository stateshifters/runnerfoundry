package docker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestStartDelegatesComposeConfigAndRunnerRequest(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeLauncher{
		commandRunner: commandRunner,
		memberID:      "member-a",
		config: ComposeConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}

	err := provider.Start(context.Background(), execution.StartRequest{
		RunnerPool: "linux",
		RunnerName: "linux-1",
		Bootstrap:  core.NewBootstrap([]byte("secret")),
	})
	if err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	expectedConfig := ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"}
	expectedRequest := composeRunnerRequest{
		RunnerPoolName: "linux",
		RunnerName:     "linux-1",
		EndpointID:     "docker:member-a",
		Bootstrap:      "secret",
	}
	if !reflect.DeepEqual(commandRunner.startConfig, expectedConfig) {
		t.Fatalf("unexpected compose config: %#v", commandRunner.startConfig)
	}
	if !reflect.DeepEqual(commandRunner.startRequest, expectedRequest) {
		t.Fatalf("unexpected runner request: %#v", commandRunner.startRequest)
	}
}

func TestListRecognizesCurrentAndLegacyOwnershipMarkers(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		output: `{"ID":"1","State":"running","RunnerPool":"linux","RunnerName":"runner-1"}
{"ID":"2","State":"exited","LegacyRunnerPool":"linux","RunnerName":"runner-2"}
{"ID":"3","State":"removing","LegacyRunnerPool":"linux","RunnerName":"runner-3"}
{"ID":"4","State":"dead","LegacyRunnerPool":"linux","RunnerName":"runner-4"}
{"ID":"5","State":"running","LegacyRunnerPool":"other"}`,
	}
	provider := Inventory{
		commandRunner: commandRunner,
	}

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 5 {
		t.Fatalf("expected 5 owned instances, got %#v", instances)
	}
	if instances[0].State != execution.RunnerStateRunning ||
		instances[1].State != execution.RunnerStateCompleted ||
		instances[2].State != execution.RunnerStateCompleted ||
		instances[3].State != execution.RunnerStateUnknown ||
		instances[4].State != execution.RunnerStateRunning {
		t.Fatalf("unexpected states: %#v", instances)
	}
}

func TestListClassifiesUnknownComposeContainerState(t *testing.T) {
	provider := Inventory{
		commandRunner: &recordingCommandRunner{
			output: `{"ID":"1","State":"restarting","LegacyRunnerPool":"linux","RunnerName":"runner-1"}`,
		},
	}

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 1 || instances[0].State != execution.RunnerStateUnknown {
		t.Fatalf("expected unknown state, got %#v", instances)
	}
}

func TestListIgnoresContainersWithoutRunnerFoundryOwnership(t *testing.T) {
	provider := Inventory{
		commandRunner: &recordingCommandRunner{
			output: `{"ID":"unowned","State":"running"}
{"ID":"owned","State":"running","LegacyRunnerPool":"linux","RunnerName":"linux-1"}`,
		},
	}

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []execution.RunnerInstance{{
		ID:         "owned",
		RunnerName: "linux-1",
		RunnerPool: "linux",
		State:      execution.RunnerStateRunning,
	}}
	if !reflect.DeepEqual(instances, want) {
		t.Fatalf("List() instances = %#v, want %#v", instances, want)
	}
}

func TestListSupportsLineDelimitedJSONAndEmptyOutput(t *testing.T) {
	provider := Inventory{
		commandRunner: &recordingCommandRunner{output: `{"ID":"1","State":"running","LegacyRunnerPool":"linux","RunnerName":"runner-1"}`},
	}
	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected line json success, got %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected one instance, got %#v", instances)
	}

	provider.commandRunner = &recordingCommandRunner{}
	instances, err = provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected empty output success, got %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("expected no instances, got %#v", instances)
	}
}

func TestDockerProviderReportsStartCommandFailure(t *testing.T) {
	provider := ComposeLauncher{commandRunner: failingCommandRunner{}}
	if err := provider.Start(context.Background(), execution.StartRequest{}); err == nil {
		t.Fatal("expected start error")
	}
}

func TestDockerProviderReportsListCommandFailure(t *testing.T) {
	provider := Inventory{commandRunner: failingCommandRunner{}}
	if _, err := provider.Inspect(context.Background()); err == nil {
		t.Fatal("expected list command error")
	}
}

func TestDockerProviderRejectsMalformedLineDelimitedJSON(t *testing.T) {
	provider := Inventory{commandRunner: &recordingCommandRunner{output: `{`}}
	if _, err := provider.Inspect(context.Background()); err == nil {
		t.Fatal("expected list json line error")
	}
}

func TestDockerProviderReportsDestroyCommandFailure(t *testing.T) {
	provider := Inventory{commandRunner: failingCommandRunner{}}
	if err := provider.Destroy(context.Background(), execution.RunnerInstance{State: execution.RunnerStateRunning}); err == nil {
		t.Fatal("expected destroy error")
	}
}

func TestDockerProviderReportsValidationCommandFailure(t *testing.T) {
	provider := ComposeLauncher{commandRunner: failingCommandRunner{}}
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("expected validate error")
	}
}

func TestDockerProviderValidationRequiresConfiguredService(t *testing.T) {
	provider := ComposeLauncher{
		commandRunner: &recordingCommandRunner{output: "other\n"},
		config:        ComposeConfig{Service: "runner"},
	}
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("expected missing service error")
	}
}

func TestDockerInventoryOwnsEndpointCompatibility(t *testing.T) {
	provider := NewInventory(&recordingCommandRunner{}, " MEMBER-A ")
	endpointID := execution.InventoryEndpointID("docker:member-a")
	legacyEndpoint := execution.InventoryEndpointID("docker-compose:member-a?compose=compose.yaml&service=runner")
	if provider.EndpointID() != endpointID || !provider.KnowsEndpoint(endpointID) || !provider.KnowsEndpoint(legacyEndpoint) {
		t.Fatalf("endpoint identity = %q", provider.EndpointID())
	}
	if !provider.CanInspectEndpointForMember(legacyEndpoint, "member-a") ||
		provider.CanInspectEndpointForMember(legacyEndpoint, "member-b") {
		t.Fatal("Docker endpoint inspection ownership is inconsistent")
	}
	if provider.KnowsEndpoint("docker-compose:member-a") || provider.KnowsEndpoint("docker-compose:%zz?compose=x") {
		t.Fatal("invalid legacy endpoint was accepted")
	}
	if _, err := provider.InspectEndpoint(context.Background(), "docker:member-b"); err == nil {
		t.Fatal("unknown endpoint was accepted")
	}
}

func TestDockerInventoryRefreshesAndInspectsKnownEndpoint(t *testing.T) {
	provider := NewInventory(&recordingCommandRunner{output: `{"ID":"1","State":"running","RunnerPool":"linux","RunnerName":"runner-1"}`}, "member-a")
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	instances, err := provider.InspectEndpoint(context.Background(), provider.EndpointID())
	if err != nil || len(instances) != 1 || instances[0].EndpointID != provider.EndpointID() {
		t.Fatalf("endpoint inventory = (%#v, %v)", instances, err)
	}
}

func TestDestroyAndValidateSucceed(t *testing.T) {
	commandRunner := &recordingCommandRunner{output: "runner\n"}
	provider := Inventory{
		commandRunner: commandRunner,
	}
	if err := provider.Destroy(context.Background(), execution.RunnerInstance{ID: "abc", RunnerName: "runner", State: execution.RunnerStateRunning}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
	if got := commandRunner.commands[0]; !reflect.DeepEqual(got.Args, []string{"rm", "--force", "abc"}) {
		t.Fatalf("unexpected running runner removal command: %#v", got)
	}
	launcher := ComposeLauncher{commandRunner: commandRunner, config: ComposeConfig{
		ComposeFile: "compose.yaml",
		Service:     "runner",
	}}
	if err := launcher.Validate(context.Background()); err != nil {
		t.Fatalf("expected validate success, got %v", err)
	}
}

func TestDestroyForceRemovesEveryActionableDockerState(t *testing.T) {
	for _, state := range []string{"running", "exited", "removing", "dead", "created", "paused", "restarting", "unexpected"} {
		t.Run(state, func(t *testing.T) { assertActionableDockerStateIsForceRemoved(t, state) })
	}
}

func assertActionableDockerStateIsForceRemoved(t *testing.T, state string) {
	t.Helper()
	commandRunner := &recordingCommandRunner{
		output: `{"ID":"runner-id","State":"` + state +
			`","RunnerPool":"linux","RunnerName":"linux-1"}`,
	}
	provider := Inventory{commandRunner: commandRunner}

	instances, err := provider.Inspect(context.Background())
	if err != nil || len(instances) != 1 {
		t.Fatalf("expected one %s runner, got %#v and error %v", state, instances, err)
	}
	if err := provider.Destroy(context.Background(), instances[0]); err != nil {
		t.Fatalf("expected %s runner destroy to succeed, got %v", state, err)
	}
	expectedRemoval := []string{"rm", "--force", "runner-id"}
	if len(commandRunner.commands) != 2 || !reflect.DeepEqual(commandRunner.commands[1].Args, expectedRemoval) {
		t.Fatalf("expected %s runner removal command %#v, got %#v", state, expectedRemoval, commandRunner.commands)
	}
}

func TestDestroyConfirmsRemovalWhenContainerIsAbsentAfterCommandFailure(t *testing.T) {
	commandRunner := &recordingCommandRunner{removeError: errors.New("removal already in progress")}
	provider := Inventory{commandRunner: commandRunner}

	err := provider.Destroy(context.Background(), execution.RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      execution.RunnerStateRunning,
	})
	if err != nil {
		t.Fatalf("expected absent container to confirm removal, got %v", err)
	}
	expectedCommands := []Command{
		{Args: []string{"rm", "--force", "runner-id"}},
		{Args: []string{"ps", "--all", "--no-trunc", "--format", dockerContainerFormat}},
	}
	if !reflect.DeepEqual(commandRunner.commands, expectedCommands) {
		t.Fatalf("unexpected cleanup commands: %#v", commandRunner.commands)
	}
}

func TestDestroyRetainsRemovalFailureWhileContainerStillExists(t *testing.T) {
	for _, dockerState := range []string{"exited", "removing"} {
		t.Run(dockerState, func(t *testing.T) {
			commandRunner := &recordingCommandRunner{
				removeError: errors.New("removal already in progress"),
				output: `{"ID":"runner-id","State":"` + dockerState +
					`","RunnerPool":"linux","RunnerName":"linux-1"}`,
			}
			provider := Inventory{commandRunner: commandRunner}

			err := provider.Destroy(context.Background(), execution.RunnerInstance{
				ID:         "runner-id",
				RunnerName: "linux-1",
				State:      execution.RunnerStateRunning,
			})
			if err == nil {
				t.Fatalf("expected %s container to retain removal failure", dockerState)
			}
		})
	}
}

func TestInspectPreservesStructuredLabelValues(t *testing.T) {
	provider := Inventory{commandRunner: &recordingCommandRunner{output: `{"ID":"runner-id","State":"running","RunnerPool":"linux, gpu=x ","LegacyRunnerPool":"","RunnerName":"runner-1","Endpoint":""}`}}

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(instances) != 1 || instances[0].RunnerPool != "linux, gpu=x " {
		t.Fatalf("Inspect() instances = %#v", instances)
	}
}

func TestInspectRecognizesCurrentLegacyAndMatchingEndpointOwnership(t *testing.T) {
	provider := NewInventory(&recordingCommandRunner{output: `{"ID":"current","State":"running","RunnerPool":"linux","RunnerName":"current","Endpoint":"docker:member-a"}
{"ID":"legacy","State":"running","LegacyRunnerPool":"linux","RunnerName":"legacy"}
{"ID":"other-endpoint","State":"running","RunnerPool":"linux","RunnerName":"other","Endpoint":"docker:member-b"}
{"ID":"unowned","State":"running","Endpoint":"docker:member-a"}`}, "member-a")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(instances) != 2 || instances[0].ID != "current" || instances[1].ID != "legacy" {
		t.Fatalf("Inspect() instances = %#v", instances)
	}
}

func TestInspectRejectsIncompleteDockerObservation(t *testing.T) {
	provider := Inventory{commandRunner: &recordingCommandRunner{
		output: `{"ID":"runner-id","RunnerPool":"linux","RunnerName":"runner-1"}`,
	}}

	if _, err := provider.Inspect(context.Background()); err == nil {
		t.Fatal("incomplete Docker observation was accepted")
	}
}

func TestDestroyRetainsRemovalFailureWhenContainerRemainsActionable(t *testing.T) {
	removalFailure := errors.New("permission denied")
	commandRunner := &recordingCommandRunner{
		removeError: removalFailure,
		output:      `{"ID":"runner-id","State":"running","RunnerPool":"linux","RunnerName":"linux-1"}`,
	}
	provider := Inventory{commandRunner: commandRunner}

	err := provider.Destroy(context.Background(), execution.RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      execution.RunnerStateRunning,
	})
	if !errors.Is(err, removalFailure) {
		t.Fatalf("expected original removal failure, got %v", err)
	}
}

func TestDestroyReportsRemovalAndConfirmationFailures(t *testing.T) {
	removalFailure := errors.New("permission denied")
	confirmationFailure := errors.New("docker unavailable")
	provider := Inventory{commandRunner: &recordingCommandRunner{
		removeError: removalFailure,
		listError:   confirmationFailure,
	}}

	err := provider.Destroy(context.Background(), execution.RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      execution.RunnerStateRunning,
	})
	if !errors.Is(err, removalFailure) {
		t.Fatalf("expected removal failure, got %v", err)
	}
	if !errors.Is(err, confirmationFailure) {
		t.Fatalf("expected confirmation failure, got %v", err)
	}
}

type recordingCommandRunner struct {
	commands     []Command
	output       string
	listError    error
	removeError  error
	startConfig  ComposeConfig
	startRequest composeRunnerRequest
}

type Command struct {
	Args []string
}

func (runner *recordingCommandRunner) record(command Command) string {
	runner.commands = append(runner.commands, command)
	return runner.output
}

func (runner *recordingCommandRunner) startComposeRunner(_ context.Context, config ComposeConfig, request composeRunnerRequest) error {
	runner.startConfig = config
	runner.startRequest = request
	return nil
}

func (runner *recordingCommandRunner) listComposeRunners(_ context.Context) (string, error) {
	output := runner.record(Command{Args: []string{"ps", "--all", "--no-trunc", "--format", dockerContainerFormat}})
	return output, runner.listError
}

func (runner *recordingCommandRunner) removeComposeRunner(_ context.Context, containerID string) error {
	runner.record(Command{Args: []string{"rm", "--force", containerID}})
	return runner.removeError
}

func (runner *recordingCommandRunner) listComposeServices(_ context.Context, config ComposeConfig) (string, error) {
	return runner.record(Command{Args: append(composeArgs(config), "config", "--services")}), nil
}

type failingCommandRunner struct{}

func (runner failingCommandRunner) startComposeRunner(context.Context, ComposeConfig, composeRunnerRequest) error {
	return errors.New("command failed")
}
func (runner failingCommandRunner) listComposeRunners(context.Context) (string, error) {
	return "", errors.New("command failed")
}
func (runner failingCommandRunner) removeComposeRunner(context.Context, string) error {
	return errors.New("command failed")
}
func (runner failingCommandRunner) listComposeServices(context.Context, ComposeConfig) (string, error) {
	return "", errors.New("command failed")
}
