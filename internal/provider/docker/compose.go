package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

const (
	jitConfigEnvironment    = "ACTIONS_RUNNER_INPUT_JITCONFIG"
	ownershipRunnerPool     = "runnerfoundry.runner-pool"
	legacyOwnershipScaleSet = "runnerfoundry.scale-set"
	ownershipRunner         = "runnerfoundry.runner"
)

type ComposeRunnerRequest struct {
	RunnerPoolName string
	RunnerName     string
	JITConfig      string
}

type ComposeConfig struct {
	ComposeFile string
	Service     string
}

type CommandRunner interface {
	StartComposeRunner(ctx context.Context, config ComposeConfig, request ComposeRunnerRequest) error
	ListComposeRunners(ctx context.Context) (string, error)
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

func (runner ExecCommandRunner) StartComposeRunner(
	ctx context.Context,
	config ComposeConfig,
	request ComposeRunnerRequest,
) error {
	invocation := dockerInvocation{
		arguments: append(composeArgs(config),
			"run", "--detach", "--rm",
			"--name", request.RunnerName,
			"--label", ownershipRunnerPool+"="+request.RunnerPoolName,
			"--label", ownershipRunner+"="+request.RunnerName,
			"--env", jitConfigEnvironment,
			config.Service,
		),
		privateEnvironment: []privateEnvironmentVariable{{name: jitConfigEnvironment, value: request.JITConfig}},
	}
	_, err := runner.runDocker(ctx, invocation)
	return err
}

func (runner ExecCommandRunner) ListComposeRunners(ctx context.Context) (string, error) {
	return runner.runDocker(ctx, dockerInvocation{arguments: []string{"ps", "--all", "--format", "json"}})
}

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

type ComposeProvider struct {
	commandRunner CommandRunner
	config        ComposeConfig
	memberID      core.MemberID
}

var _ execution.ExecutionProvider = ComposeProvider{}

func NewComposeProvider(commandRunner CommandRunner, config ComposeConfig, memberID core.MemberID) ComposeProvider {
	return ComposeProvider{commandRunner: commandRunner, config: config, memberID: memberID}
}

func (provider ComposeProvider) EndpointID() execution.InventoryEndpointID {
	return execution.InventoryEndpointID("docker:" + url.QueryEscape(normalizedHostname(provider.memberID)))
}

func (provider ComposeProvider) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	if provider.EndpointID() == endpointID {
		return true
	}
	hostname, legacy := legacyComposeEndpointHostname(endpointID)
	return legacy && hostname == normalizedHostname(provider.memberID)
}

func normalizedHostname(memberID core.MemberID) string {
	return strings.ToLower(strings.TrimSpace(string(memberID)))
}

func legacyComposeEndpointHostname(endpointID execution.InventoryEndpointID) (string, bool) {
	legacyEndpoint, legacy := strings.CutPrefix(string(endpointID), "docker-compose:")
	if !legacy {
		return "", false
	}
	escapedHostname, _, hasConfiguration := strings.Cut(legacyEndpoint, "?")
	if !hasConfiguration {
		return "", false
	}
	hostname, err := url.QueryUnescape(escapedHostname)
	return normalizedHostname(core.MemberID(hostname)), err == nil
}

func (provider ComposeProvider) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	memberID core.MemberID,
) bool {
	return provider.KnowsEndpoint(endpointID) && normalizedHostname(provider.memberID) == normalizedHostname(memberID)
}

func (provider ComposeProvider) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}

func (provider ComposeProvider) InspectEndpoint(
	ctx context.Context,
	endpointID execution.InventoryEndpointID,
) ([]execution.RunnerInstance, error) {
	if !provider.KnowsEndpoint(endpointID) {
		return nil, fmt.Errorf("docker Compose endpoint %q is not configured", endpointID)
	}
	instances, err := provider.Inspect(ctx)
	for index := range instances {
		instances[index].EndpointID = endpointID
	}
	return instances, err
}

func (provider ComposeProvider) Validate(ctx context.Context) error {
	output, err := provider.commandRunner.ListComposeServices(ctx, provider.config)
	if err != nil {
		return fmt.Errorf("validate Docker Compose provider: validate compose config: %w", err)
	}
	for _, service := range strings.Split(output, "\n") {
		if strings.TrimSpace(service) == provider.config.Service {
			return nil
		}
	}
	return fmt.Errorf("validate Docker Compose provider: compose service %q was not found", provider.config.Service)
}

func (provider ComposeProvider) Start(ctx context.Context, request execution.StartRequest) error {
	err := provider.commandRunner.StartComposeRunner(ctx, provider.config, ComposeRunnerRequest{
		RunnerPoolName: string(request.RunnerPool),
		RunnerName:     string(request.RunnerName),
		JITConfig:      string(request.Bootstrap.Content()),
	})
	if err != nil {
		return fmt.Errorf("start Docker Compose runner: start compose runner: %w", err)
	}
	return nil
}

func (provider ComposeProvider) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	output, err := provider.commandRunner.ListComposeRunners(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker Compose runners: list compose runners: %w", err)
	}
	containers, err := decodeDockerContainers(output)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker Compose runners: %w", err)
	}
	instances := make([]execution.RunnerInstance, 0, len(containers))
	for _, container := range containers {
		instance, owned := runnerInstance(container)
		if owned {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func runnerInstance(container dockerContainer) (execution.RunnerInstance, bool) {
	labels := parseLabels(container.Labels)
	runnerPoolName := labels[ownershipRunnerPool]
	if runnerPoolName == "" {
		runnerPoolName = labels[legacyOwnershipScaleSet]
	}
	if runnerPoolName == "" {
		return execution.RunnerInstance{}, false
	}
	return execution.RunnerInstance{
		ID:         container.ID,
		RunnerName: core.RunnerName(labels[ownershipRunner]),
		RunnerPool: core.RunnerPool(runnerPoolName),
		State:      dockerState(container.State),
	}, true
}

func (provider ComposeProvider) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	if instance.State == execution.RunnerStateCompleted {
		return nil
	}
	err := provider.commandRunner.RemoveComposeRunner(ctx, instance.ID)
	if err == nil {
		return nil
	}
	removalFailure := fmt.Errorf("destroy Docker Compose runner: remove docker runner %q: %w", instance.RunnerName, err)
	instances, confirmationErr := provider.Inspect(ctx)
	if confirmationErr != nil {
		return errors.Join(removalFailure, fmt.Errorf("confirm docker runner %q removal: %w", instance.RunnerName, confirmationErr))
	}
	if dockerCleanupComplete(instances, instance.ID) {
		return nil
	}
	return removalFailure
}

func dockerCleanupComplete(instances []execution.RunnerInstance, containerID string) bool {
	for _, instance := range instances {
		if instance.ID == containerID {
			return instance.State == execution.RunnerStateCompleted
		}
	}
	return true
}

func composeArgs(config ComposeConfig) []string { return []string{"compose", "-f", config.ComposeFile} }

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

func dockerState(state string) execution.RunnerState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "running":
		return execution.RunnerStateRunning
	case "exited", "removing":
		return execution.RunnerStateCompleted
	default:
		return execution.RunnerStateUnknown
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
