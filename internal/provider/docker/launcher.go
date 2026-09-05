package docker

import (
	"context"
	"fmt"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

type ComposeLauncher struct {
	commandRunner composeLauncherCommands
	config        ComposeConfig
	memberID      core.MemberID
}

var _ execution.RunnerLauncher = ComposeLauncher{}

func NewComposeLauncher(commandRunner composeLauncherCommands, config ComposeConfig, memberID core.MemberID) ComposeLauncher {
	return ComposeLauncher{commandRunner: commandRunner, config: config, memberID: memberID}
}

func (launcher ComposeLauncher) EndpointID() execution.InventoryEndpointID {
	return dockerEndpointID(launcher.memberID)
}

func (launcher ComposeLauncher) Validate(ctx context.Context) error {
	output, err := launcher.commandRunner.listComposeServices(ctx, launcher.config)
	if err != nil {
		return fmt.Errorf("validate Docker Compose provider: validate compose config: %w", err)
	}
	for _, service := range strings.Split(output, "\n") {
		if strings.TrimSpace(service) == launcher.config.Service {
			return nil
		}
	}
	return fmt.Errorf("validate Docker Compose provider: compose service %q was not found", launcher.config.Service)
}

func (launcher ComposeLauncher) Start(ctx context.Context, request execution.StartRequest) error {
	err := launcher.commandRunner.startComposeRunner(ctx, launcher.config, composeRunnerRequest{
		RunnerPoolName: string(request.RunnerPool),
		RunnerName:     string(request.RunnerName),
		EndpointID:     string(launcher.EndpointID()),
		Bootstrap:      string(request.Bootstrap.Content()),
	})
	if err != nil {
		return fmt.Errorf("start Docker Compose runner: start compose runner: %w", err)
	}
	return nil
}
