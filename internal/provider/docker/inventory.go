package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

type Inventory struct {
	commandRunner dockerInventoryCommands
	memberID      core.MemberID
}

var _ execution.RunnerInventory = Inventory{}

func NewInventory(commandRunner dockerInventoryCommands, memberID core.MemberID) Inventory {
	return Inventory{commandRunner: commandRunner, memberID: memberID}
}

func (inventory Inventory) EndpointID() execution.InventoryEndpointID {
	return dockerEndpointID(inventory.memberID)
}

func dockerEndpointID(memberID core.MemberID) execution.InventoryEndpointID {
	return execution.InventoryEndpointID("docker:" + url.QueryEscape(normalizedHostname(memberID)))
}

func (inventory Inventory) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	if inventory.EndpointID() == endpointID {
		return true
	}
	hostname, legacy := legacyComposeEndpointHostname(endpointID)
	return legacy && hostname == normalizedHostname(inventory.memberID)
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

func (inventory Inventory) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	memberID core.MemberID,
) bool {
	return inventory.KnowsEndpoint(endpointID) && normalizedHostname(inventory.memberID) == normalizedHostname(memberID)
}

func (inventory Inventory) Refresh(ctx context.Context) error {
	_, err := inventory.Inspect(ctx)
	return err
}

func (inventory Inventory) InspectEndpoint(
	ctx context.Context,
	endpointID execution.InventoryEndpointID,
) ([]execution.RunnerInstance, error) {
	if !inventory.KnowsEndpoint(endpointID) {
		return nil, fmt.Errorf("docker endpoint %q is not configured", endpointID)
	}
	instances, err := inventory.Inspect(ctx)
	for index := range instances {
		instances[index].EndpointID = endpointID
	}
	return instances, err
}

func (inventory Inventory) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	output, err := inventory.commandRunner.listComposeRunners(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker runners: list Docker runners: %w", err)
	}
	containers, err := decodeDockerContainers(output)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker runners: %w", err)
	}
	instances := make([]execution.RunnerInstance, 0, len(containers))
	for _, container := range containers {
		instance, owned := inventory.runnerInstance(container)
		if owned {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func (inventory Inventory) runnerInstance(container dockerContainer) (execution.RunnerInstance, bool) {
	runnerPoolName := container.RunnerPool
	if runnerPoolName == "" {
		runnerPoolName = container.LegacyRunnerPool
	}
	if runnerPoolName == "" {
		return execution.RunnerInstance{}, false
	}
	if container.Endpoint != "" && container.Endpoint != string(inventory.EndpointID()) {
		return execution.RunnerInstance{}, false
	}
	return execution.RunnerInstance{
		ID:         container.ID,
		RunnerName: core.RunnerName(container.RunnerName),
		RunnerPool: core.RunnerPool(runnerPoolName),
		State:      dockerState(container.State),
	}, true
}

func (inventory Inventory) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	err := inventory.commandRunner.removeComposeRunner(ctx, instance.ID)
	if err == nil {
		return nil
	}
	removalFailure := fmt.Errorf("destroy Docker runner: remove Docker runner %q: %w", instance.RunnerName, err)
	instances, confirmationErr := inventory.Inspect(ctx)
	if confirmationErr != nil {
		return errors.Join(removalFailure, fmt.Errorf("confirm Docker runner %q removal: %w", instance.RunnerName, confirmationErr))
	}
	if dockerRunnerAbsent(instances, instance.ID) {
		return nil
	}
	return removalFailure
}

func dockerRunnerAbsent(instances []execution.RunnerInstance, containerID string) bool {
	for _, instance := range instances {
		if instance.ID == containerID {
			return false
		}
	}
	return true
}

func decodeDockerContainers(output string) ([]dockerContainer, error) {
	trimmedOutput := strings.TrimSpace(output)
	if trimmedOutput == "" {
		return nil, nil
	}
	containers := make([]dockerContainer, 0, strings.Count(trimmedOutput, "\n")+1)
	for _, line := range strings.Split(trimmedOutput, "\n") {
		container, err := decodeDockerContainer(line)
		if err != nil {
			return nil, err
		}
		containers = append(containers, container)
	}
	return containers, nil
}

func decodeDockerContainer(line string) (dockerContainer, error) {
	var container dockerContainer
	if err := json.Unmarshal([]byte(line), &container); err != nil {
		return dockerContainer{}, fmt.Errorf("decode docker ps JSON line: %w", err)
	}
	if container.ID == "" || container.State == "" {
		return dockerContainer{}, errors.New("decode docker ps JSON: container ID and state are required")
	}
	return container, nil
}

type dockerContainer struct {
	ID               string `json:"ID"`
	State            string `json:"State"`
	RunnerPool       string `json:"RunnerPool"`
	LegacyRunnerPool string `json:"LegacyRunnerPool"`
	RunnerName       string `json:"RunnerName"`
	Endpoint         string `json:"Endpoint"`
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
