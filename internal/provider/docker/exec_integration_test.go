package docker

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestExecCommandRunnerBuildsDockerCommandsInMemory(t *testing.T) {
	recording := &recordingDockerExecution{output: []byte("runner\n")}
	runner := recording.runner()

	output, err := runner.ListComposeServices(context.Background(), ComposeConfig{ComposeFile: "compose.yml"})
	if err != nil || strings.TrimSpace(output) != "runner" {
		t.Fatalf("list compose services: output=%q error=%v", output, err)
	}
	assertDockerArguments(t, recording.arguments, "compose", "-f", "compose.yml", "config", "--services")

	if _, err = runner.ListComposeRunners(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	assertDockerArguments(t, recording.arguments, "ps", "--all", "--format", "json", "--filter", "label=runnerfoundry.scale-set=linux")

	if err = runner.RemoveComposeRunner(context.Background(), "runner-id"); err != nil {
		t.Fatal(err)
	}
	assertDockerArguments(t, recording.arguments, "rm", "--force", "runner-id")
}

func TestExecCommandRunnerBuildsStartCommandAndRedactsFailure(t *testing.T) {
	recording := &recordingDockerExecution{
		output: []byte("docker diagnostic: opaque-jit-value"),
		err:    errors.New("failed"),
	}
	err := recording.runner().StartComposeRunner(context.Background(), ComposeConfig{Service: "runner"}, StartRequest{
		ScaleSetName: "linux", RunnerName: "linux-1", JITConfig: "opaque-jit-value",
	})
	if err == nil || strings.Contains(err.Error(), "opaque-jit-value") || !strings.Contains(err.Error(), "docker diagnostic: [REDACTED]") {
		t.Fatalf("docker failure = %v", err)
	}
	assertDockerArguments(t, recording.arguments,
		"compose", "-f", "", "run", "--detach", "--rm", "--name", "linux-1",
		"--label", "runnerfoundry.scale-set=linux", "--label", "runnerfoundry.runner=linux-1",
		"--env", "ACTIONS_RUNNER_INPUT_JITCONFIG=opaque-jit-value", "runner",
	)
}

func TestExecCommandRunnerReportsFailureWithoutDiagnostics(t *testing.T) {
	recording := &recordingDockerExecution{err: errors.New("missing")}
	if _, err := recording.runner().ListComposeRunners(context.Background(), "linux"); err == nil || strings.Contains(err.Error(), ": missing:") {
		t.Fatalf("docker failure = %v", err)
	}
}

func TestNewExecCommandRunnerProvidesProductionDependencies(t *testing.T) {
	runner := NewExecCommandRunner()
	if runner.newCommand == nil || runner.combinedOutput == nil {
		t.Fatal("production command dependencies are incomplete")
	}
}

type recordingDockerExecution struct {
	arguments []string
	output    []byte
	err       error
}

func (recording *recordingDockerExecution) runner() ExecCommandRunner {
	return ExecCommandRunner{
		newCommand: func(_ context.Context, name string, arguments ...string) *exec.Cmd {
			recording.arguments = append([]string{name}, arguments...)
			return &exec.Cmd{}
		},
		combinedOutput: func(*exec.Cmd) ([]byte, error) { return recording.output, recording.err },
	}
}

func assertDockerArguments(t *testing.T, actual []string, expected ...string) {
	t.Helper()
	if !slices.Equal(actual, append([]string{"docker"}, expected...)) {
		t.Fatalf("docker arguments = %#v, want %#v", actual, append([]string{"docker"}, expected...))
	}
}
