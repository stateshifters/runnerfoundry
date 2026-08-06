package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/meza/RunnerFoundry/internal/provider/contract"
)

type RunnerState string

const (
	JITConfigEnvironment = contract.JITConfigEnvironment
	RunnerStateRunning   = contract.RunnerStateRunning
	RunnerStateCompleted = contract.RunnerStateCompleted
	RunnerStateUnknown   = contract.RunnerStateUnknown
)

type RunnerInstance struct {
	ID           string
	RunnerName   string
	ScaleSetName string
	State        RunnerState
}

type StartRequest struct {
	ScaleSetName string
	RunnerName   string
	JITConfig    string
}

type ComposeConfig struct {
	ComposeFile string
	Service     string
}

type CommandRunner interface {
	StartComposeRunner(ctx context.Context, config ComposeConfig, request StartRequest) error
	ListComposeRunners(ctx context.Context, scaleSetName string) (string, error)
	RemoveComposeRunner(ctx context.Context, containerID string) error
	ListComposeServices(ctx context.Context, config ComposeConfig) (string, error)
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

func (runner ExecCommandRunner) StartComposeRunner(ctx context.Context, config ComposeConfig, request StartRequest) error {
	invocation := dockerInvocation{
		arguments: append(composeArgs(config),
			"run", "--detach", "--rm",
			"--name", request.RunnerName,
			"--label", contract.OwnershipScaleSet+"="+request.ScaleSetName,
			"--label", contract.OwnershipRunner+"="+request.RunnerName,
			"--env", JITConfigEnvironment,
			config.Service,
		),
		privateEnvironment: []privateEnvironmentVariable{{name: JITConfigEnvironment, value: request.JITConfig}},
	}
	_, err := runner.runDocker(ctx, invocation)
	return err
}

func (runner ExecCommandRunner) ListComposeRunners(ctx context.Context, scaleSetName string) (string, error) {
	arguments := []string{"ps", "--all", "--format", "json"}
	if scaleSetName != "" {
		arguments = append(arguments, "--filter", "label="+contract.OwnershipScaleSet+"="+scaleSetName)
	}
	return runner.runDocker(ctx, dockerInvocation{arguments: arguments})
}

func (runner ExecCommandRunner) RemoveComposeRunner(ctx context.Context, containerID string) error {
	_, err := runner.runDocker(ctx, dockerInvocation{arguments: []string{"rm", "--force", containerID}})
	return err
}

func (runner ExecCommandRunner) ListComposeServices(ctx context.Context, config ComposeConfig) (string, error) {
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
			return "", fmt.Errorf("docker command failed: %w: %s", executionError, diagnostic)
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

type redactedExecutionError struct {
	message string
}

func (executionError redactedExecutionError) Error() string {
	return executionError.message
}

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

type ComposeProvider struct {
	CommandRunner CommandRunner
	Config        ComposeConfig
}

func (provider ComposeProvider) Start(ctx context.Context, request StartRequest) error {
	err := provider.CommandRunner.StartComposeRunner(ctx, provider.Config, request)
	if err != nil {
		return fmt.Errorf("start compose runner: %w", err)
	}
	return nil
}

func (provider ComposeProvider) List(ctx context.Context, requestedPools ...string) ([]RunnerInstance, error) {
	output, err := provider.CommandRunner.ListComposeRunners(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list compose runners: %w", err)
	}
	containers, err := decodeDockerContainers(output)
	if err != nil {
		return nil, err
	}
	instances := make([]RunnerInstance, 0, len(containers))
	for _, container := range containers {
		labels := parseLabels(container.Labels)
		scaleSetName := labels[contract.OwnershipScaleSet]
		if scaleSetName == "" {
			continue
		}
		if len(requestedPools) != 0 && scaleSetName != requestedPools[0] {
			continue
		}
		reportedScaleSet := scaleSetName
		if len(requestedPools) != 0 {
			reportedScaleSet = ""
		}
		instances = append(instances, RunnerInstance{
			ID:           container.ID,
			RunnerName:   labels[contract.OwnershipRunner],
			ScaleSetName: reportedScaleSet,
			State:        dockerState(container.State),
		})
	}
	return instances, nil
}

func (provider ComposeProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	err := provider.CommandRunner.RemoveComposeRunner(ctx, instance.ID)
	if err != nil {
		return fmt.Errorf("remove docker runner %q: %w", instance.RunnerName, err)
	}
	return nil
}

func (provider ComposeProvider) Validate(ctx context.Context) error {
	output, err := provider.CommandRunner.ListComposeServices(ctx, provider.Config)
	if err != nil {
		return fmt.Errorf("validate compose config: %w", err)
	}
	for _, service := range strings.Split(output, "\n") {
		if strings.TrimSpace(service) == provider.Config.Service {
			return nil
		}
	}
	return fmt.Errorf("compose service %q was not found", provider.Config.Service)
}

func composeArgs(config ComposeConfig) []string {
	return []string{"compose", "-f", config.ComposeFile}
}

func decodeDockerContainers(output string) ([]dockerContainer, error) {
	var containers []dockerContainer
	trimmedOutput := strings.TrimSpace(output)
	if strings.HasPrefix(trimmedOutput, "[") {
		if err := json.Unmarshal([]byte(trimmedOutput), &containers); err != nil {
			return nil, fmt.Errorf("decode docker ps json: %w", err)
		}
		return containers, nil
	}
	if trimmedOutput == "" {
		return containers, nil
	}
	for _, line := range strings.Split(trimmedOutput, "\n") {
		var container dockerContainer
		if err := json.Unmarshal([]byte(line), &container); err != nil {
			return nil, fmt.Errorf("decode docker ps json line: %w", err)
		}
		containers = append(containers, container)
	}
	return containers, nil
}

type dockerContainer struct {
	ID     string `json:"ID"`
	Names  string `json:"Names"`
	State  string `json:"State"`
	Labels string `json:"Labels"`
}

func dockerState(state string) RunnerState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "running":
		return RunnerStateRunning
	case "dead":
		return RunnerStateCompleted
	default:
		return RunnerStateUnknown
	}
}

func parseLabels(rawLabels string) map[string]string {
	labels := map[string]string{}
	for _, rawLabel := range strings.Split(rawLabels, ",") {
		labelParts := strings.SplitN(rawLabel, "=", 2)
		if len(labelParts) == 2 {
			labels[labelParts[0]] = labelParts[1]
		}
	}
	return labels
}
