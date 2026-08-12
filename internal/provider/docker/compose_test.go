package docker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/meza/RunnerFoundry/internal/provider/contract"
)

func TestStartDelegatesComposeConfigAndRunnerRequest(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{
		CommandRunner: commandRunner,
		Config: ComposeConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}

	err := provider.Start(context.Background(), StartRequest{
		RunnerPoolName: "linux",
		RunnerName:     "linux-1",
		JITConfig:      "secret",
	})
	if err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	expectedConfig := ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"}
	expectedRequest := StartRequest{RunnerPoolName: "linux", RunnerName: "linux-1", JITConfig: "secret"}
	if !reflect.DeepEqual(commandRunner.startConfig, expectedConfig) {
		t.Fatalf("unexpected compose config: %#v", commandRunner.startConfig)
	}
	if !reflect.DeepEqual(commandRunner.startRequest, expectedRequest) {
		t.Fatalf("unexpected runner request: %#v", commandRunner.startRequest)
	}
}

func TestListRecognizesCurrentAndLegacyOwnershipMarkers(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		output: `[
{"ID":"1","Names":"runner-1","State":"running","Labels":"runnerfoundry.runner-pool=linux,runnerfoundry.runner=runner-1"},
{"ID":"2","Names":"runner-2","State":"exited","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=runner-2"},
{"ID":"3","Names":"runner-3","State":"removing","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=runner-3"},
{"ID":"4","Names":"runner-4","State":"dead","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=runner-4"},
{"ID":"5","Names":"other","State":"running","Labels":"runnerfoundry.scale-set=other"}
]`,
	}
	provider := ComposeProvider{
		CommandRunner: commandRunner,
		Config: ComposeConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}

	instances, err := provider.List(context.Background(), "linux")
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 4 {
		t.Fatalf("expected 4 owned instances, got %#v", instances)
	}
	if instances[0].State != RunnerStateRunning ||
		instances[1].State != RunnerStateCompleted ||
		instances[2].State != RunnerStateCompleted ||
		instances[3].State != RunnerStateUnknown {
		t.Fatalf("unexpected states: %#v", instances)
	}
}

func TestListClassifiesUnknownComposeContainerState(t *testing.T) {
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{
			output: `{"ID":"1","Names":"runner-1","State":"restarting","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=runner-1"}`,
		},
	}

	instances, err := provider.List(context.Background(), "linux")
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 1 || instances[0].State != RunnerStateUnknown {
		t.Fatalf("expected unknown state, got %#v", instances)
	}
}

func TestListIgnoresContainersWithoutRunnerFoundryOwnership(t *testing.T) {
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{
			output: `[
{"ID":"unowned","Names":"unowned","State":"running","Labels":"com.example.kind=runner"},
{"ID":"owned","Names":"owned","State":"running","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=linux-1"}
]`,
		},
	}

	instances, err := provider.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []RunnerInstance{{
		ID:             "owned",
		RunnerName:     "linux-1",
		RunnerPoolName: "linux",
		State:          RunnerStateRunning,
	}}
	if !reflect.DeepEqual(instances, want) {
		t.Fatalf("List() instances = %#v, want %#v", instances, want)
	}
}

func TestListSupportsLineDelimitedJSONAndEmptyOutput(t *testing.T) {
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{output: `{"ID":"1","Names":"runner-1","State":"running","Labels":"runnerfoundry.scale-set=linux,runnerfoundry.runner=runner-1"}`},
		Config:        ComposeConfig{},
	}
	instances, err := provider.List(context.Background(), "linux")
	if err != nil {
		t.Fatalf("expected line json success, got %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected one instance, got %#v", instances)
	}

	provider.CommandRunner = &recordingCommandRunner{}
	instances, err = provider.List(context.Background(), "linux")
	if err != nil {
		t.Fatalf("expected empty output success, got %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("expected no instances, got %#v", instances)
	}
}

func TestDockerProviderReportsStartCommandFailure(t *testing.T) {
	provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
	if err := provider.Start(context.Background(), StartRequest{}); err == nil {
		t.Fatal("expected start error")
	}
}

func TestDockerProviderReportsListCommandFailure(t *testing.T) {
	provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
	if _, err := provider.List(context.Background(), "linux"); err == nil {
		t.Fatal("expected list command error")
	}
}

func TestDockerProviderRejectsMalformedJSONArray(t *testing.T) {
	provider := ComposeProvider{CommandRunner: &recordingCommandRunner{output: `[`}}
	if _, err := provider.List(context.Background(), "linux"); err == nil {
		t.Fatal("expected list json error")
	}
}

func TestDockerProviderRejectsMalformedLineDelimitedJSON(t *testing.T) {
	provider := ComposeProvider{CommandRunner: &recordingCommandRunner{output: `{`}}
	if _, err := provider.List(context.Background(), "linux"); err == nil {
		t.Fatal("expected list json line error")
	}
}

func TestDockerProviderReportsDestroyCommandFailure(t *testing.T) {
	provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
	if err := provider.Destroy(context.Background(), RunnerInstance{State: RunnerStateRunning}); err == nil {
		t.Fatal("expected destroy error")
	}
}

func TestDockerProviderReportsValidationCommandFailure(t *testing.T) {
	provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("expected validate error")
	}
}

func TestDockerProviderValidationRequiresConfiguredService(t *testing.T) {
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{output: "other\n"},
		Config:        ComposeConfig{Service: "runner"},
	}
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("expected missing service error")
	}
}

func TestDestroyAndValidateSucceed(t *testing.T) {
	commandRunner := &recordingCommandRunner{output: "runner\n"}
	provider := ComposeProvider{
		CommandRunner: commandRunner,
		Config: ComposeConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}
	if err := provider.Destroy(context.Background(), RunnerInstance{ID: "abc", RunnerName: "runner", State: RunnerStateRunning}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
	if got := commandRunner.commands[0]; !reflect.DeepEqual(got.Args, []string{"rm", "--force", "abc"}) {
		t.Fatalf("unexpected running runner removal command: %#v", got)
	}
	if err := provider.Validate(context.Background()); err != nil {
		t.Fatalf("expected validate success, got %v", err)
	}
}

func TestDestroyRemovesTransitionalOrUnknownRunner(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{CommandRunner: commandRunner}

	if err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateUnknown,
	}); err != nil {
		t.Fatalf("expected unknown runner destroy to succeed, got %v", err)
	}
	expected := []string{"rm", "--force", "runner-id"}
	if len(commandRunner.commands) != 1 || !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("expected unknown runner removal command %#v, got %#v", expected, commandRunner.commands)
	}
}

func TestDestroyForceRemovesEveryActionableDockerState(t *testing.T) {
	for _, state := range []string{"running", "dead", "created", "paused", "restarting", "unexpected"} {
		t.Run(state, func(t *testing.T) { assertActionableDockerStateIsForceRemoved(t, state) })
	}
}

func assertActionableDockerStateIsForceRemoved(t *testing.T, state string) {
	t.Helper()
	commandRunner := &recordingCommandRunner{
		output: `{"ID":"runner-id","Names":"linux-1","State":"` + state +
			`","Labels":"runnerfoundry.runner-pool=linux,runnerfoundry.runner=linux-1"}`,
	}
	provider := ComposeProvider{CommandRunner: commandRunner}

	instances, err := provider.List(context.Background(), "linux")
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

func TestDestroyLeavesCompletedRunnerToDocker(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{CommandRunner: commandRunner}

	if err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateCompleted,
	}); err != nil {
		t.Fatalf("expected completed runner destroy to succeed, got %v", err)
	}
	if len(commandRunner.commands) != 0 {
		t.Fatalf("expected Docker to own completed runner removal, got commands %#v", commandRunner.commands)
	}
}

func TestDestroyConfirmsRemovalWhenContainerIsAbsentAfterCommandFailure(t *testing.T) {
	commandRunner := &recordingCommandRunner{removeError: errors.New("removal already in progress")}
	provider := ComposeProvider{CommandRunner: commandRunner}

	err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateRunning,
	})
	if err != nil {
		t.Fatalf("expected absent container to confirm removal, got %v", err)
	}
	expectedCommands := []Command{
		{Args: []string{"rm", "--force", "runner-id"}},
		{Args: []string{"ps", "--all", "--format", "json", "--filter", "label=" + contract.OwnershipRunnerPool + "="}},
	}
	if !reflect.DeepEqual(commandRunner.commands, expectedCommands) {
		t.Fatalf("unexpected cleanup commands: %#v", commandRunner.commands)
	}
}

func TestDestroyConfirmsRemovalWhenDockerOwnsContainerCleanup(t *testing.T) {
	for _, dockerState := range []string{"exited", "removing"} {
		t.Run(dockerState, func(t *testing.T) {
			commandRunner := &recordingCommandRunner{
				removeError: errors.New("removal already in progress"),
				output: `{"ID":"runner-id","Names":"linux-1","State":"` + dockerState +
					`","Labels":"runnerfoundry.runner-pool=linux,runnerfoundry.runner=linux-1"}`,
			}
			provider := ComposeProvider{CommandRunner: commandRunner}

			err := provider.Destroy(context.Background(), RunnerInstance{
				ID:         "runner-id",
				RunnerName: "linux-1",
				State:      RunnerStateRunning,
			})
			if err != nil {
				t.Fatalf("expected %s container to confirm Docker-owned cleanup, got %v", dockerState, err)
			}
		})
	}
}

func TestDestroyRetainsRemovalFailureWhenContainerRemainsActionable(t *testing.T) {
	removalFailure := errors.New("permission denied")
	commandRunner := &recordingCommandRunner{
		removeError: removalFailure,
		output: `{"ID":"runner-id","Names":"linux-1","State":"running",` +
			`"Labels":"runnerfoundry.runner-pool=linux,runnerfoundry.runner=linux-1"}`,
	}
	provider := ComposeProvider{CommandRunner: commandRunner}

	err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateRunning,
	})
	if !errors.Is(err, removalFailure) {
		t.Fatalf("expected original removal failure, got %v", err)
	}
}

func TestDestroyReportsRemovalAndConfirmationFailures(t *testing.T) {
	removalFailure := errors.New("permission denied")
	confirmationFailure := errors.New("docker unavailable")
	provider := ComposeProvider{CommandRunner: &recordingCommandRunner{
		removeError: removalFailure,
		listError:   confirmationFailure,
	}}

	err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateRunning,
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
	startRequest StartRequest
}

type Command struct {
	Args []string
}

func (runner *recordingCommandRunner) record(command Command) string {
	runner.commands = append(runner.commands, command)
	return runner.output
}

func (runner *recordingCommandRunner) StartComposeRunner(_ context.Context, config ComposeConfig, request StartRequest) error {
	runner.startConfig = config
	runner.startRequest = request
	return nil
}

func (runner *recordingCommandRunner) ListComposeRunners(_ context.Context, runnerPoolName string) (string, error) {
	output := runner.record(Command{Args: []string{"ps", "--all", "--format", "json", "--filter", "label=" + contract.OwnershipRunnerPool + "=" + runnerPoolName}})
	return output, runner.listError
}

func (runner *recordingCommandRunner) RemoveComposeRunner(_ context.Context, containerID string) error {
	runner.record(Command{Args: []string{"rm", "--force", containerID}})
	return runner.removeError
}

func (runner *recordingCommandRunner) ListComposeServices(_ context.Context, config ComposeConfig) (string, error) {
	return runner.record(Command{Args: append(composeArgs(config), "config", "--services")}), nil
}

type failingCommandRunner struct{}

func (runner failingCommandRunner) StartComposeRunner(context.Context, ComposeConfig, StartRequest) error {
	return errors.New("command failed")
}
func (runner failingCommandRunner) ListComposeRunners(context.Context, string) (string, error) {
	return "", errors.New("command failed")
}
func (runner failingCommandRunner) RemoveComposeRunner(context.Context, string) error {
	return errors.New("command failed")
}
func (runner failingCommandRunner) ListComposeServices(context.Context, ComposeConfig) (string, error) {
	return "", errors.New("command failed")
}
