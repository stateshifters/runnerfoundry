package docker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestExecCommandRunnerBuildsDockerCommandsInMemory(t *testing.T) {
	recording := &recordingDockerExecution{output: []byte("runner\n")}
	runner := recording.runner()

	output, err := runner.ListComposeServices(context.Background(), ComposeConfig{ComposeFile: "compose.yml"})
	if err != nil || strings.TrimSpace(output) != "runner" {
		t.Fatalf("list compose services: output=%q error=%v", output, err)
	}
	assertDockerArguments(t, recording.arguments, "compose", "-f", "compose.yml", "config", "--services")

	if _, err = runner.ListComposeRunners(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertDockerArguments(t, recording.arguments, "ps", "--all", "--format", "json")

	if err = runner.RemoveComposeRunner(context.Background(), "runner-id"); err != nil {
		t.Fatal(err)
	}
	assertDockerArguments(t, recording.arguments, "rm", "--force", "runner-id")
}

func TestExecCommandRunnerAcceptsDockerOwnedRemoval(t *testing.T) {
	const containerID = "runner-id"
	recording := &recordingDockerExecution{
		output: []byte("removal of container " + containerID + " is already in progress"),
		err:    errors.New("exit status 1"),
	}

	err := recording.runner().RemoveComposeRunner(context.Background(), containerID)
	if err != nil {
		t.Fatalf("Docker-owned removal returned an error: %v", err)
	}
}

func TestExecCommandRunnerAcceptsPrefixedDockerOwnedRemoval(t *testing.T) {
	const containerID = "runner-id"
	recording := &recordingDockerExecution{
		output: []byte("Error response from daemon: removal of container " + containerID + " is already in progress"),
		err:    errors.New("exit status 1"),
	}

	err := recording.runner().RemoveComposeRunner(context.Background(), containerID)
	if err != nil {
		t.Fatalf("Docker-owned removal returned an error: %v", err)
	}
}

func TestExecCommandRunnerRejectsNonExactRemovalDiagnostics(t *testing.T) {
	const containerID = "runner-id"
	diagnostics := map[string]string{
		"different container": "removal of container other-id is already in progress",
		"partial":             "removal of container " + containerID,
		"extended":            "removal of container " + containerID + " is already in progress; retry later",
		"permission failure":  "permission denied",
	}

	for name, diagnostic := range diagnostics {
		t.Run(name, func(t *testing.T) {
			recording := &recordingDockerExecution{output: []byte(diagnostic), err: errors.New("exit status 1")}
			if err := recording.runner().RemoveComposeRunner(context.Background(), containerID); err == nil {
				t.Fatalf("non-exact Docker diagnostic %q was accepted", diagnostic)
			}
		})
	}
}

func TestComposeProviderDoesNotRefreshDockerOwnedRemoval(t *testing.T) {
	const containerID = "runner-id"
	recording := &recordingDockerExecution{
		output: []byte("Error response from daemon: removal of container " + containerID + " is already in progress"),
		err:    errors.New("exit status 1"),
	}
	provider := ComposeProvider{commandRunner: recording.runner()}

	err := provider.Destroy(context.Background(), execution.RunnerInstance{
		ID:         containerID,
		RunnerName: "linux-1",
		State:      execution.RunnerStateRunning,
	})
	if err != nil {
		t.Fatalf("Docker-owned removal returned an error: %v", err)
	}
	assertDockerArguments(t, recording.arguments, "rm", "--force", containerID)
}

func TestExecCommandRunnerKeepsJITConfigPrivate(t *testing.T) {
	testCases := []jitPrivacyTestCase{
		{name: "ordinary", jitConfig: "opaque-jit-value"},
		{name: "empty", jitConfig: ""},
		{name: "multiline", jitConfig: "first line\nsecond line"},
		{name: "shell significant", jitConfig: `$(echo exposed); '&|%PATH%' "quoted"`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assertJITConfigPrivacy(t, testCase.jitConfig)
		})
	}
}

type jitPrivacyTestCase struct {
	name      string
	jitConfig string
}

func assertJITConfigPrivacy(t *testing.T, jitConfig string) {
	t.Helper()
	parentEnvironment := []string{"RUNNERFOUNDRY_PRESERVED=inherited", jitConfigEnvironment + "=parent-jit-value"}
	recording := &recordingDockerExecution{
		echoCommandEnvironment: true,
		inheritedEnvironment:   parentEnvironment,
	}
	provider := ComposeProvider{commandRunner: recording.runner(), config: ComposeConfig{Service: "runner"}}

	err := provider.Start(context.Background(), execution.StartRequest{
		RunnerPool: "linux", RunnerName: "linux-1", Bootstrap: core.NewBootstrap([]byte(jitConfig)),
	})
	if err == nil {
		t.Fatal("expected docker failure")
	}
	assertPrivateJITArguments(t, recording.arguments, jitConfig)
	assertPrivateJITEnvironment(t, recording.commandEnvironment, parentEnvironment, jitConfig)
	assertJITFailureRedacted(t, err, jitConfig)
}

func assertPrivateJITArguments(t *testing.T, arguments []string, jitConfig string) {
	t.Helper()
	assertDockerArguments(t, arguments,
		"compose", "-f", "", "run", "--detach", "--rm", "--name", "linux-1",
		"--label", "runnerfoundry.runner-pool=linux", "--label", "runnerfoundry.runner=linux-1",
		"--env", jitConfigEnvironment, "runner",
	)
	if jitConfig != "" && strings.Contains(fmt.Sprint(arguments), jitConfig) {
		t.Fatalf("docker arguments disclosed JIT config: %#v", arguments)
	}
}

func assertJITFailureRedacted(t *testing.T, err error, jitConfig string) {
	t.Helper()
	if jitConfig == "" {
		return
	}
	if strings.Contains(err.Error(), jitConfig) {
		t.Fatalf("docker failure disclosed JIT config: %q", err.Error())
	}
	if strings.Count(err.Error(), "[REDACTED]") < 2 {
		t.Fatalf("docker failure did not redact diagnostic and execution errors: %q", err.Error())
	}
}

func TestExecCommandRunnerReportsFailureWithoutDiagnostics(t *testing.T) {
	recording := &recordingDockerExecution{err: errors.New("missing")}
	if _, err := recording.runner().ListComposeRunners(context.Background()); err == nil || strings.Contains(err.Error(), ": missing:") {
		t.Fatalf("docker failure = %v", err)
	}
}

func TestNewExecCommandRunnerInitializesCommandBoundaries(t *testing.T) {
	runner := NewExecCommandRunner()
	if runner.newCommand == nil || runner.combinedOutput == nil || runner.inheritedEnvironment == nil {
		t.Fatal("production command dependencies are incomplete")
	}
}

type recordingDockerExecution struct {
	arguments              []string
	commandEnvironment     []string
	output                 []byte
	err                    error
	echoCommandEnvironment bool
	inheritedEnvironment   []string
}

func (recording *recordingDockerExecution) runner() ExecCommandRunner {
	return ExecCommandRunner{
		newCommand: func(_ context.Context, name string, arguments ...string) *exec.Cmd {
			recording.arguments = append([]string{name}, arguments...)
			return &exec.Cmd{}
		},
		inheritedEnvironment: func() []string { return slices.Clone(recording.inheritedEnvironment) },
		combinedOutput: func(command *exec.Cmd) ([]byte, error) {
			recording.commandEnvironment = slices.Clone(command.Env)
			if recording.echoCommandEnvironment {
				environment := strings.Join(command.Env, "\n")
				return []byte("docker diagnostic: " + environment), errors.New("docker execution failed: " + environment)
			}
			return recording.output, recording.err
		},
	}
}

func assertPrivateJITEnvironment(t *testing.T, actual []string, inherited []string, jitConfig string) {
	t.Helper()
	expected := make([]string, 0, len(inherited))
	prefix := jitConfigEnvironment + "="
	for _, entry := range inherited {
		if !strings.HasPrefix(entry, prefix) {
			expected = append(expected, entry)
		}
	}
	expected = append(expected, prefix+jitConfig)
	if !slices.Equal(actual, expected) {
		t.Fatalf("docker environment = %#v, want %#v", actual, expected)
	}
}

func assertDockerArguments(t *testing.T, actual []string, expected ...string) {
	t.Helper()
	if !slices.Equal(actual, append([]string{"docker"}, expected...)) {
		t.Fatalf("docker arguments = %#v, want %#v", actual, append([]string{"docker"}, expected...))
	}
}
