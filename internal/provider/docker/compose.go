package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/provider/contract"
)

// RunnerState is Docker's normalized lifecycle observation.
type RunnerState string

const (
	JITConfigEnvironment = contract.JITConfigEnvironment
	RunnerStateRunning   = contract.RunnerStateRunning
	RunnerStateCompleted = contract.RunnerStateCompleted
	RunnerStateUnknown   = contract.RunnerStateUnknown
)

// RunnerInstance is one owned Compose container returned by List.
type RunnerInstance struct {
	ID             string
	RunnerName     string
	RunnerPoolName string
	State          RunnerState
}

// StartRequest contains the ownership labels and opaque JIT configuration for
// one ephemeral runner. JITConfig is passed through child-only environment.
type StartRequest struct {
	RunnerPoolName string
	RunnerName     string
	JITConfig      string
}

// ComposeConfig selects one service from one Compose file.
type ComposeConfig struct {
	ComposeFile string
	Service     string
}

// CommandRunner is the Docker command boundary. Implementations return command
// failures without exposing private JIT values.
type CommandRunner interface {
	StartComposeRunner(ctx context.Context, config ComposeConfig, request StartRequest) error
	ListComposeRunners(ctx context.Context, runnerPoolName string) (string, error)
	RemoveComposeRunner(ctx context.Context, containerID string) error
	ListComposeServices(ctx context.Context, config ComposeConfig) (string, error)
}

// ExecCommandRunner invokes the local Docker CLI without retaining command or
// environment state between calls.
type ExecCommandRunner struct {
	newCommand           func(context.Context, string, ...string) *exec.Cmd
	combinedOutput       func(*exec.Cmd) ([]byte, error)
	inheritedEnvironment func() []string
}

// NewExecCommandRunner constructs the production Docker CLI boundary.
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
			"--label", contract.OwnershipRunnerPool+"="+request.RunnerPoolName,
			"--label", contract.OwnershipRunner+"="+request.RunnerName,
			"--env", JITConfigEnvironment,
			config.Service,
		),
		privateEnvironment: []privateEnvironmentVariable{{name: JITConfigEnvironment, value: request.JITConfig}},
	}
	_, err := runner.runDocker(ctx, invocation)
	return err
}

func (runner ExecCommandRunner) ListComposeRunners(ctx context.Context, runnerPoolName string) (string, error) {
	arguments := []string{"ps", "--all", "--format", "json"}
	if runnerPoolName != "" {
		arguments = append(arguments, "--filter", "label="+contract.OwnershipRunnerPool+"="+runnerPoolName)
	}
	return runner.runDocker(ctx, dockerInvocation{arguments: arguments})
}

// RemoveComposeRunner requests forced cleanup and treats Docker's exact
// removal-in-progress response as a successful handoff to the daemon.
func (runner ExecCommandRunner) RemoveComposeRunner(ctx context.Context, containerID string) error {
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

// ComposeProvider owns runner containers bearing RunnerFoundry ownership
// labels for Config. Callers must Validate before Start.
type ComposeProvider struct {
	CommandRunner CommandRunner
	Config        ComposeConfig
}

// Start creates one detached, self-removing runner container.
func (provider ComposeProvider) Start(ctx context.Context, request StartRequest) error {
	err := provider.CommandRunner.StartComposeRunner(ctx, provider.Config, request)
	if err != nil {
		return fmt.Errorf("start compose runner: %w", err)
	}
	return nil
}

// List returns all owned containers when requestedPools is empty, or filters by
// the first requested Runner Pool. A filtered result omits RunnerPoolName.
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
		instance, owned := runnerInstance(container, requestedPools)
		if !owned {
			continue
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

func runnerInstance(container dockerContainer, requestedPools []string) (RunnerInstance, bool) {
	labels := parseLabels(container.Labels)
	runnerPoolName := labels[contract.OwnershipRunnerPool]
	if runnerPoolName == "" {
		runnerPoolName = labels[contract.LegacyOwnershipScaleSet]
	}
	if runnerPoolName == "" || len(requestedPools) != 0 && runnerPoolName != requestedPools[0] {
		return RunnerInstance{}, false
	}
	reportedRunnerPool := runnerPoolName
	if len(requestedPools) != 0 {
		reportedRunnerPool = ""
	}
	return RunnerInstance{
		ID:             container.ID,
		RunnerName:     labels[contract.OwnershipRunner],
		RunnerPoolName: reportedRunnerPool,
		State:          dockerState(container.State),
	}, true
}

// Destroy confirms cleanup for one Docker runner. Completed instances are left
// to Docker's automatic --rm lifecycle; running and unknown instances are
// force-removed. If forced removal fails, Destroy refreshes owned inventory and
// succeeds only when the exact container is absent or completed. A failed
// refresh is joined with the original removal failure.
func (provider ComposeProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	if instance.State == RunnerStateCompleted {
		return nil
	}
	err := provider.CommandRunner.RemoveComposeRunner(ctx, instance.ID)
	if err == nil {
		return nil
	}
	return provider.confirmRemoval(ctx, instance, fmt.Errorf("remove docker runner %q: %w", instance.RunnerName, err))
}

func (provider ComposeProvider) confirmRemoval(
	ctx context.Context,
	instance RunnerInstance,
	removalFailure error,
) error {
	instances, confirmationErr := provider.List(ctx)
	if confirmationErr != nil {
		return errors.Join(
			removalFailure,
			fmt.Errorf("confirm docker runner %q removal: %w", instance.RunnerName, confirmationErr),
		)
	}
	if dockerCleanupComplete(instances, instance.ID) {
		return nil
	}
	return removalFailure
}

func dockerCleanupComplete(instances []RunnerInstance, containerID string) bool {
	for _, instance := range instances {
		if instance.ID == containerID {
			return instance.State == RunnerStateCompleted
		}
	}
	return true
}

// Validate resolves the Compose model and requires Config.Service to exist.
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
	case "exited", "removing":
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
