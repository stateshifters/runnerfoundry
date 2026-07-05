package docker

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestStartUsesDockerComposeRunWithJITConfigAndOwnershipLabels(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := ComposeProvider{
		CommandRunner: commandRunner,
		Config: ComposeConfig{
			ComposeFile:          "compose.yaml",
			Service:              "runner",
			JITConfigEnvironment: "RUNNER_JIT_CONFIG",
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
		"--env", "RUNNER_JIT_CONFIG=secret",
		"runner",
	}
	if !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("unexpected docker command\nwant %#v\ngot  %#v", expected, commandRunner.commands[0].Args)
	}
}

func TestListClassifiesRunningAndCompletedComposeContainers(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		output: `[
{"ID":"1","Names":"runner-1","State":"running","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-1"},
{"ID":"2","Names":"runner-2","State":"exited","Labels":"github-scaler.scale-set=linux,github-scaler.runner=runner-2"},
{"ID":"3","Names":"other","State":"running","Labels":"github-scaler.scale-set=other"}
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
	if len(instances) != 2 {
		t.Fatalf("expected 2 owned instances, got %#v", instances)
	}
	if instances[0].State != RunnerStateRunning || instances[1].State != RunnerStateCompleted {
		t.Fatalf("unexpected states: %#v", instances)
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
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if err := provider.Start(context.Background(), StartRequest{}); err == nil {
				t.Fatal("expected start error")
			}
		},
		"list command": func(t *testing.T) {
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if _, err := provider.List(context.Background(), "linux"); err == nil {
				t.Fatal("expected list command error")
			}
		},
		"list array json": func(t *testing.T) {
			provider := ComposeProvider{CommandRunner: &recordingCommandRunner{output: `[`}}
			if _, err := provider.List(context.Background(), "linux"); err == nil {
				t.Fatal("expected list json error")
			}
		},
		"list line json": func(t *testing.T) {
			provider := ComposeProvider{CommandRunner: &recordingCommandRunner{output: `{`}}
			if _, err := provider.List(context.Background(), "linux"); err == nil {
				t.Fatal("expected list json line error")
			}
		},
		"destroy": func(t *testing.T) {
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if err := provider.Destroy(context.Background(), RunnerInstance{}); err == nil {
				t.Fatal("expected destroy error")
			}
		},
		"validate": func(t *testing.T) {
			provider := ComposeProvider{CommandRunner: failingCommandRunner{}}
			if err := provider.Validate(context.Background()); err == nil {
				t.Fatal("expected validate error")
			}
		},
		"validate missing service": func(t *testing.T) {
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
	provider := ComposeProvider{
		CommandRunner: &recordingCommandRunner{output: "runner\n"},
		Config: ComposeConfig{
			ComposeFile: "compose.yaml",
			Service:     "runner",
		},
	}
	if err := provider.Destroy(context.Background(), RunnerInstance{ID: "abc", RunnerName: "runner"}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
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
