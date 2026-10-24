package docker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const (
	bootstrapEnvironment    = "ACTIONS_RUNNER_INPUT_JITCONFIG"
	ownershipRunnerPool     = "runnerfoundry.runner-pool"
	legacyOwnershipScaleSet = "runnerfoundry.scale-set"
	ownershipRunner         = "runnerfoundry.runner"
	ownershipEndpoint       = "runnerfoundry.endpoint"
	dockerContainerFormat   = `{"ID":{{json .ID}},"State":{{json .State}},"RunnerPool":{{json (.Label "runnerfoundry.runner-pool")}},"LegacyRunnerPool":{{json (.Label "runnerfoundry.scale-set")}},"RunnerName":{{json (.Label "runnerfoundry.runner")}},"Endpoint":{{json (.Label "runnerfoundry.endpoint")}}}`
)

type composeRunnerRequest struct {
	RunnerPoolName string
	RunnerName     string
	EndpointID     string
	Bootstrap      string
}

type ComposeConfig struct {
	ComposeFile string
	Service     string
}

type composeLauncherCommands interface {
	startComposeRunner(ctx context.Context, config ComposeConfig, request composeRunnerRequest) error
	listComposeServices(ctx context.Context, config ComposeConfig) (string, error)
}

type dockerInventoryCommands interface {
	listComposeRunners(ctx context.Context) (string, error)
	removeComposeRunner(ctx context.Context, containerID string) error
}

type ExecCommandRunner struct {
	newCommand           func(context.Context, string, ...string) *exec.Cmd
	combinedOutput       func(*exec.Cmd) ([]byte, error)
	inheritedEnvironment func() []string
}

func NewExecCommandRunner() ExecCommandRunner {
	return ExecCommandRunner{
		newCommand:           exec.CommandContext,
		combinedOutput:       (*exec.Cmd).CombinedOutput,
		inheritedEnvironment: os.Environ,
	}
}

func (runner ExecCommandRunner) startComposeRunner(
	ctx context.Context,
	config ComposeConfig,
	request composeRunnerRequest,
) error {
	invocation := dockerInvocation{
		arguments: append(composeArgs(config),
			"run", "--detach", "--rm",
			"--name", request.RunnerName,
			"--label", ownershipRunnerPool+"="+request.RunnerPoolName,
			"--label", ownershipRunner+"="+request.RunnerName,
			"--label", ownershipEndpoint+"="+request.EndpointID,
			"--env", bootstrapEnvironment,
			config.Service,
		),
		privateEnvironment: []privateEnvironmentVariable{{name: bootstrapEnvironment, value: request.Bootstrap}},
	}
	_, err := runner.runDocker(ctx, invocation)
	return err
}

func (runner ExecCommandRunner) listComposeRunners(ctx context.Context) (string, error) {
	return runner.runDocker(ctx, dockerInvocation{
		arguments: []string{"ps", "--all", "--no-trunc", "--format", dockerContainerFormat},
	})
}

func (runner ExecCommandRunner) removeComposeRunner(ctx context.Context, containerID string) error {
	diagnostic, err := runner.runDocker(ctx, dockerInvocation{arguments: []string{"rm", "--force", containerID}})
	if err != nil && dockerOwnsRemoval(diagnostic, containerID) {
		return nil
	}
	return err
}

func dockerOwnsRemoval(diagnostic string, containerID string) bool {
	diagnostic = strings.TrimPrefix(diagnostic, "Error response from daemon: ")
	return diagnostic == "removal of container "+containerID+" is already in progress"
}

func (runner ExecCommandRunner) listComposeServices(ctx context.Context, config ComposeConfig) (string, error) {
	return runner.runDocker(ctx, dockerInvocation{arguments: append(composeArgs(config), "config", "--services")})
}

func (runner ExecCommandRunner) runDocker(ctx context.Context, invocation dockerInvocation) (string, error) {
	execCommand := runner.newCommand(ctx, "docker", invocation.arguments...)
	if len(invocation.privateEnvironment) > 0 {
		execCommand.Env = replaceEnvironment(runner.inheritedEnvironment(), invocation.privateEnvironment)
	}
	outputBytes, err := runner.combinedOutput(execCommand)
	if err != nil {
		diagnostic := redactPrivateValues(strings.TrimSpace(string(outputBytes)), invocation.privateEnvironment)
		executionError := redactCommandError(err, invocation.privateEnvironment)
		if diagnostic != "" {
			return diagnostic, fmt.Errorf("docker command failed: %w: %s", executionError, diagnostic)
		}
		return "", fmt.Errorf("docker command failed: %w", executionError)
	}
	return string(outputBytes), nil
}

type dockerInvocation struct {
	arguments          []string
	privateEnvironment []privateEnvironmentVariable
}

type privateEnvironmentVariable struct {
	name  string
	value string
}

type redactedExecutionError struct{ message string }

func (executionError redactedExecutionError) Error() string { return executionError.message }

func replaceEnvironment(inherited []string, replacements []privateEnvironmentVariable) []string {
	replacedNames := make(map[string]struct{}, len(replacements))
	for _, replacement := range replacements {
		replacedNames[replacement.name] = struct{}{}
	}
	environment := make([]string, 0, len(inherited)+len(replacements))
	for _, entry := range inherited {
		name, _, hasValue := strings.Cut(entry, "=")
		if _, replaced := replacedNames[name]; hasValue && replaced {
			continue
		}
		environment = append(environment, entry)
	}
	for _, replacement := range replacements {
		environment = append(environment, replacement.name+"="+replacement.value)
	}
	return environment
}

func redactCommandError(err error, privateEnvironment []privateEnvironmentVariable) error {
	redactedMessage := redactPrivateValues(err.Error(), privateEnvironment)
	if redactedMessage == err.Error() {
		return err
	}
	return redactedExecutionError{message: redactedMessage}
}

func redactPrivateValues(value string, privateEnvironment []privateEnvironmentVariable) string {
	redactedValue := value
	for _, environmentVariable := range privateEnvironment {
		if environmentVariable.value != "" {
			redactedValue = strings.ReplaceAll(redactedValue, environmentVariable.value, "[REDACTED]")
		}
	}
	return redactedValue
}

func composeArgs(config ComposeConfig) []string { return []string{"compose", "-f", config.ComposeFile} }
