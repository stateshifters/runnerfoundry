//go:build e2e

package daemon

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
)

func TestProductContractApplicationReadinessWaitsForProviderValidation(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	application, commands := newProductionProviderReadinessApplication(t)
	validation := commands.blockValidation(t)
	running := startE2EApplication(t, application)
	awaitSignal(t, "blocked production provider validation", validation.started)
	assertApplicationNotReady(t, application, "while provider validation is blocked")
	validation.Release()
	application.awaitReady(t, "readiness after provider validation")
	application.tickers.awaitInitialGeneration(t)
	stopReadinessApplication(t, running)
}

func TestProductContractApplicationReadinessDoesNotWaitForInventory(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	application, commands := newProductionProviderReadinessApplication(t)
	inventory := commands.blockInventory(t)
	running := startE2EApplication(t, application)
	awaitSignal(t, "blocked initial production inventory", inventory.started)
	application.awaitReady(t, "readiness while inventory remains blocked")
	inventory.Release()
	application.tickers.awaitInitialGeneration(t)
	stopReadinessApplication(t, running)
}

func newProductionProviderReadinessApplication(
	t *testing.T,
) (*e2eApplication, *readinessDockerCommands) {
	t.Helper()
	application := newE2EApplication(t)
	application.configured.RunnerPools = application.configured.RunnerPools[:1]
	application.configured.RunnerPools[0].Docker.ComposeFile = "compose.yaml"
	commands := &readinessDockerCommands{delegate: newProductionCompositionDockerCommands()}
	application.useProductionProviders = true
	application.newDockerLauncher = func(configured dockerprovider.ComposeConfig, memberID core.MemberID) execution.RunnerLauncher {
		return commands.newLauncher(configured, memberID)
	}
	application.newDockerInventory = func(memberID core.MemberID) execution.RunnerInventory {
		return commands.newInventory(memberID)
	}
	return application, commands
}

func assertApplicationNotReady(t *testing.T, application *e2eApplication, phase string) {
	t.Helper()
	select {
	case <-application.notifier.ready:
		t.Fatalf("application reported READY %s", phase)
	default:
	}
}

func stopReadinessApplication(t *testing.T, running e2eApplicationRun) {
	t.Helper()
	running.cancel()
	if err := awaitApplicationResult(t, running.result); err != nil {
		t.Fatalf("stop provider-readiness application: %v", err)
	}
}

type readinessCommandGate struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newReadinessCommandGate(t *testing.T) *readinessCommandGate {
	t.Helper()
	gate := &readinessCommandGate{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.Release)
	return gate
}

func (gate *readinessCommandGate) wait(ctx context.Context) error {
	gate.startedOnce.Do(func() { close(gate.started) })
	select {
	case <-gate.release:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("readiness command gate canceled: %w", ctx.Err())
	}
}

func (gate *readinessCommandGate) Release() {
	gate.releaseOnce.Do(func() { close(gate.release) })
}

type readinessDockerCommands struct {
	mutex          sync.Mutex
	delegate       *productionCompositionDockerCommands
	validationGate *readinessCommandGate
	inventoryGate  *readinessCommandGate
}

func (commands *readinessDockerCommands) blockValidation(t *testing.T) *readinessCommandGate {
	t.Helper()
	gate := newReadinessCommandGate(t)
	commands.mutex.Lock()
	commands.validationGate = gate
	commands.mutex.Unlock()
	return gate
}

func (commands *readinessDockerCommands) blockInventory(t *testing.T) *readinessCommandGate {
	t.Helper()
	gate := newReadinessCommandGate(t)
	commands.mutex.Lock()
	commands.inventoryGate = gate
	commands.mutex.Unlock()
	return gate
}

func (commands *readinessDockerCommands) takeValidationGate() *readinessCommandGate {
	commands.mutex.Lock()
	defer commands.mutex.Unlock()
	gate := commands.validationGate
	commands.validationGate = nil
	return gate
}

func (commands *readinessDockerCommands) takeInventoryGate() *readinessCommandGate {
	commands.mutex.Lock()
	defer commands.mutex.Unlock()
	gate := commands.inventoryGate
	commands.inventoryGate = nil
	return gate
}

func (commands *readinessDockerCommands) newLauncher(
	configured dockerprovider.ComposeConfig,
	memberID core.MemberID,
) readinessDockerLauncher {
	return readinessDockerLauncher{RunnerLauncher: commands.delegate.newLauncher(configured, memberID), commands: commands}
}

func (commands *readinessDockerCommands) newInventory(memberID core.MemberID) readinessDockerInventory {
	return readinessDockerInventory{RunnerInventory: commands.delegate.newInventory(memberID), commands: commands}
}

type readinessDockerLauncher struct {
	execution.RunnerLauncher
	commands *readinessDockerCommands
}

func (launcher readinessDockerLauncher) Validate(ctx context.Context) error {
	if gate := launcher.commands.takeValidationGate(); gate != nil {
		if err := gate.wait(ctx); err != nil {
			return err
		}
	}
	if err := launcher.RunnerLauncher.Validate(ctx); err != nil {
		return fmt.Errorf("validate Docker launcher: %w", err)
	}
	return nil
}

type readinessDockerInventory struct {
	execution.RunnerInventory
	commands *readinessDockerCommands
}

func (inventory readinessDockerInventory) Refresh(ctx context.Context) error {
	if err := inventory.wait(ctx); err != nil {
		return err
	}
	if err := inventory.RunnerInventory.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh Docker inventory: %w", err)
	}
	return nil
}

func (inventory readinessDockerInventory) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	if err := inventory.wait(ctx); err != nil {
		return nil, err
	}
	instances, err := inventory.RunnerInventory.Inspect(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect Docker inventory: %w", err)
	}
	return instances, nil
}

func (inventory readinessDockerInventory) wait(ctx context.Context) error {
	if gate := inventory.commands.takeInventoryGate(); gate != nil {
		if err := gate.wait(ctx); err != nil {
			return err
		}
	}
	return nil
}
