package docker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStartUsesDockerComposeRunWithJITConfigAndOwnershipLabels(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{
		CommandRunner: commandRunner,
		Config: ComposeConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}

	err := provider.Start(context.Background(), StartRequest{
		ScaleSetName: "linux",
		RunnerName:   "linux-1",
		JITConfig:    "secret",
	})
	if err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	expected := []string{
		"compose", "-f", "compose.yaml",
		"run", "--detach", "--rm", "--name", "linux-1",
		"--label", "github-scaler.scale-set=linux",
		"--label", "github-scaler.runner=linux-1",
		"--env", "ACTIONS_RUNNER_INPUT_JITCONFIG=secret",
		"runner",
	}
	if !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("unexpected docker command\nwant %#v\ngot  %#v", expected, commandRunner.commands[0].Args)
	}
}

func TestListClassifiesComposeContainerStates(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		output: `[
{"ID":"1","Names":"runner-1","State":"running","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-1"},
{"ID":"2","Names":"runner-2","State":"exited","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-2"},
{"ID":"3","Names":"runner-3","State":"removing","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-3"},
{"ID":"4","Names":"runner-4","State":"dead","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-4"},
{"ID":"5","Names":"other","State":"running","Labels":"github-scaler.scale-set=other"}
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
		instances[1].State != RunnerStateUnknown ||
		instances[2].State != RunnerStateUnknown ||
		instances[3].State != RunnerStateCompleted {
		t.Fatalf("unexpected states: %#v", instances)
	}
}

func TestListClassifiesUnknownComposeContainerState(t *testing.T) {
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{
			output: `{"ID":"1","Names":"runner-1","State":"restarting","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-1"}`,
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

func TestListSupportsLineDelimitedJSONAndEmptyOutput(t *testing.T) {
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{output: `{"ID":"1","Names":"runner-1","State":"running","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-1"}`},
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

func TestDockerProviderSurfacesCommandAndJSONFailures(t *testing.T) {
	tests := map[string]func(t *testing.T){
		"start": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if err := provider.Start(context.Background(), StartRequest{}); err == nil {
				t.Fatal("expected start error")
			}
		},
		"list command": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if _, err := provider.List(context.Background(), "linux"); err == nil {
				t.Fatal("expected list command error")
			}
		},
		"list array json": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{CommandRunner: &recordingCommandRunner{output: `[`}}
			if _, err := provider.List(context.Background(), "linux"); err == nil {
				t.Fatal("expected list json error")
			}
		},
		"list line json": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{CommandRunner: &recordingCommandRunner{output: `{`}}
			if _, err := provider.List(context.Background(), "linux"); err == nil {
				t.Fatal("expected list json line error")
			}
		},
		"destroy": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if err := provider.Destroy(context.Background(), RunnerInstance{State: RunnerStateRunning}); err == nil {
				t.Fatal("expected destroy error")
			}
		},
		"validate": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if err := provider.Validate(context.Background()); err == nil {
				t.Fatal("expected validate error")
			}
		},
		"validate missing service": func(t *testing.T) {
			t.Helper()
			provider := ComposeProvider{
				CommandRunner: &recordingCommandRunner{output: "other\n"},
				Config:        ComposeConfig{Service: "runner"},
			}
			if err := provider.Validate(context.Background()); err == nil {
				t.Fatal("expected missing service error")
			}
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, testCase)
	}
}

func TestDestroyValidateAndExecRunnerSuccess(t *testing.T) {
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

	output, err := (ExecCommandRunner{}).Run(context.Background(), Command{Name: "go", Args: []string{"version"}})
	if err != nil {
		t.Fatalf("expected exec success, got %v", err)
	}
	if output == "" {
		t.Fatal("expected go version output")
	}
	if _, err := (ExecCommandRunner{}).Run(context.Background(), Command{Name: "definitely-missing-github-scaler-command"}); err == nil {
		t.Fatal("expected exec failure")
	}
}

func TestDestroyLeavesTransitionalOrUnknownRunnerToDocker(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{CommandRunner: commandRunner}

	if err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateUnknown,
	}); err != nil {
		t.Fatalf("expected unknown runner destroy to be ignored, got %v", err)
	}
	if len(commandRunner.commands) != 0 {
		t.Fatalf("expected no docker command, got %#v", commandRunner.commands)
	}
}

func TestDestroyRemovesDeadRunner(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{CommandRunner: commandRunner}

	if err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "runner-id",
		RunnerName: "linux-1",
		State:      RunnerStateCompleted,
	}); err != nil {
		t.Fatalf("expected dead runner destroy to succeed, got %v", err)
	}
	expected := []string{"rm", "--force", "runner-id"}
	if len(commandRunner.commands) != 1 || !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("expected dead runner removal command %#v, got %#v", expected, commandRunner.commands)
	}
}

func TestExecCommandRunnerReportsDiagnosticsWithoutJITConfig(t *testing.T) {
	programPath := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(programPath, []byte(`package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprint(os.Stderr, "docker diagnostic: secret-jit-config")
	os.Exit(1)
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := (ExecCommandRunner{}).Run(context.Background(), Command{
		Name: "go",
		Args: []string{"run", programPath, "--env", JITConfigEnvironment + "=secret-jit-config"},
	})
	if err == nil {
		t.Fatal("expected command failure")
	}
	if strings.Contains(err.Error(), "secret-jit-config") {
		t.Fatalf("expected JIT config to be redacted, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "go command failed") {
		t.Fatalf("expected command context, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "docker diagnostic: [REDACTED]") {
		t.Fatalf("expected redacted command diagnostic, got %q", err.Error())
	}
}

type recordingCommandRunner struct {
	commands []Command
	output   string
}

func (runner *recordingCommandRunner) Run(ctx context.Context, command Command) (string, error) {
	runner.commands = append(runner.commands, command)
	return runner.output, nil
}

type failingCommandRunner struct{}

func (runner failingCommandRunner) Run(ctx context.Context, command Command) (string, error) {
	return "", errors.New("command failed")
}
