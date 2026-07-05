package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

type RunnerState string

const (
	RunnerStateRunning   RunnerState = "running"
	RunnerStateCompleted RunnerState = "completed"
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
	ComposeFile          string
	Service              string
	JITConfigEnvironment string
}

type Command struct {
	Name string
	Args []string
}

type CommandRunner interface {
	Run(ctx context.Context, command Command) (string, error)
}

type ExecCommandRunner struct{}

func (runner ExecCommandRunner) Run(ctx context.Context, command Command) (string, error) {
	execCommand := exec.CommandContext(ctx, command.Name, command.Args...)
	outputBytes, err := execCommand.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s command failed: %w: %s", command.Name, err, string(outputBytes))
	}
	return string(outputBytes), nil
}

type ComposeProvider struct {
	CommandRunner CommandRunner
	Config        ComposeConfig
}

func (provider ComposeProvider) Start(ctx context.Context, request StartRequest) error {
	_, err := provider.CommandRunner.Run(ctx, Command{
		Name: "docker",
		Args: append(provider.baseArgs(),
			"run", "--detach", "--rm",
			"--name", request.RunnerName,
			"--label", "github-scaler.scale-set="+request.ScaleSetName,
			"--label", "github-scaler.runner="+request.RunnerName,
			"--env", provider.Config.JITConfigEnvironment+"="+request.JITConfig,
			provider.Config.Service,
		),
	})
	if err != nil {
		return fmt.Errorf("start compose runner: %w", err)
	}
	return nil
}

func (provider ComposeProvider) List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error) {
	output, err := provider.CommandRunner.Run(ctx, Command{
		Name: "docker",
		Args: []string{"ps", "--all", "--format", "json", "--filter", "label=github-scaler.scale-set=" + scaleSetName},
	})
	if err != nil {
		return nil, fmt.Errorf("list compose runners: %w", err)
	}
	var containers []dockerContainer
	trimmedOutput := strings.TrimSpace(output)
	if strings.HasPrefix(trimmedOutput, "[") {
		if err := json.Unmarshal([]byte(trimmedOutput), &containers); err != nil {
			return nil, fmt.Errorf("decode docker ps json: %w", err)
		}
	} else if trimmedOutput != "" {
		for _, line := range strings.Split(trimmedOutput, "\n") {
			var container dockerContainer
			if err := json.Unmarshal([]byte(line), &container); err != nil {
				return nil, fmt.Errorf("decode docker ps json line: %w", err)
			}
			containers = append(containers, container)
		}
	}
	instances := make([]RunnerInstance, 0, len(containers))
	for _, container := range containers {
		labels := parseLabels(container.Labels)
		if labels["github-scaler.scale-set"] != scaleSetName {
			continue
		}
		instances = append(instances, RunnerInstance{
			ID:         container.ID,
			RunnerName: labels["github-scaler.runner"],
			State:      dockerState(container.State),
		})
	}
	return instances, nil
}

func (provider ComposeProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	_, err := provider.CommandRunner.Run(ctx, Command{
		Name: "docker",
		Args: []string{"rm", "--force", instance.ID},
	})
	if err != nil {
		return fmt.Errorf("remove docker runner %q: %w", instance.RunnerName, err)
	}
	return nil
}

func (provider ComposeProvider) Validate(ctx context.Context) error {
	output, err := provider.CommandRunner.Run(ctx, Command{
		Name: "docker",
		Args: append(provider.baseArgs(), "config", "--services"),
	})
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

func (provider ComposeProvider) baseArgs() []string {
	return []string{"compose", "-f", provider.Config.ComposeFile}
}

type dockerContainer struct {
	ID     string `json:"ID"`
	Names  string `json:"Names"`
	State  string `json:"State"`
	Labels string `json:"Labels"`
}

func dockerState(state string) RunnerState {
	if strings.EqualFold(state, "running") {
		return RunnerStateRunning
	}
	return RunnerStateCompleted
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
