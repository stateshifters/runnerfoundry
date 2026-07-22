package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/meza/github-scaler/internal/provider/contract"
)

type RunnerState string

const (
	JITConfigEnvironment = contract.JITConfigEnvironment
	RunnerStateRunning   = contract.RunnerStateRunning
	RunnerStateCompleted = contract.RunnerStateCompleted
	RunnerStateUnknown   = contract.RunnerStateUnknown
)

type RunnerInstance struct {
	ID         string
	RunnerName string
	State      RunnerState
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

type ExecCommandRunner struct{}

func (runner ExecCommandRunner) StartComposeRunner(ctx context.Context, config ComposeConfig, request StartRequest) error {
	args := append(composeArgs(config),
		"run", "--detach", "--rm",
		"--name", request.RunnerName,
		"--label", contract.OwnershipScaleSet+"="+request.ScaleSetName,
		"--label", contract.OwnershipRunner+"="+request.RunnerName,
		"--env", JITConfigEnvironment+"="+request.JITConfig,
		config.Service,
	)
	_, err := runner.runDocker(ctx, args, request.JITConfig)
	return err
}

func (runner ExecCommandRunner) ListComposeRunners(ctx context.Context, scaleSetName string) (string, error) {
	return runner.runDocker(ctx, []string{
		"ps", "--all", "--format", "json", "--filter", "label=" + contract.OwnershipScaleSet + "=" + scaleSetName,
	}, "")
}

func (runner ExecCommandRunner) RemoveComposeRunner(ctx context.Context, containerID string) error {
	_, err := runner.runDocker(ctx, []string{"rm", "--force", containerID}, "")
	return err
}

func (runner ExecCommandRunner) ListComposeServices(ctx context.Context, config ComposeConfig) (string, error) {
	return runner.runDocker(ctx, append(composeArgs(config), "config", "--services"), "")
}

func (runner ExecCommandRunner) runDocker(ctx context.Context, args []string, jitConfig string) (string, error) {
	execCommand := exec.CommandContext(ctx, "docker")
	execCommand.Args = append(execCommand.Args, args...)
	outputBytes, err := execCommand.CombinedOutput()
	if err != nil {
		diagnostic := strings.TrimSpace(string(outputBytes))
		if jitConfig != "" {
			diagnostic = strings.ReplaceAll(diagnostic, jitConfig, "[REDACTED]")
		}
		if diagnostic != "" {
			return "", fmt.Errorf("docker command failed: %w: %s", err, diagnostic)
		}
		return "", fmt.Errorf("docker command failed: %w", err)
	}
	return string(outputBytes), nil
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

func (provider ComposeProvider) List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error) {
	output, err := provider.CommandRunner.ListComposeRunners(ctx, scaleSetName)
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
		if labels[contract.OwnershipScaleSet] != scaleSetName {
			continue
		}
		instances = append(instances, RunnerInstance{
			ID:         container.ID,
			RunnerName: labels[contract.OwnershipRunner],
			State:      dockerState(container.State),
		})
	}
	return instances, nil
}

func (provider ComposeProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	switch instance.State {
	case RunnerStateRunning, RunnerStateCompleted:
	default:
		return nil
	}
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
