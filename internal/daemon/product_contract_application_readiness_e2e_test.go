//go:build e2e

package daemon

import (
	"context"
	"fmt"
	"sync"
	"testing"

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
	application.dockerCommands = commands
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

func (commands *readinessDockerCommands) StartComposeRunner(
	ctx context.Context,
	configured dockerprovider.ComposeConfig,
	request dockerprovider.ComposeRunnerRequest,
) error {
	return commands.delegate.StartComposeRunner(ctx, configured, request)
}

func (commands *readinessDockerCommands) ListComposeRunners(
	ctx context.Context,
) (string, error) {
	if gate := commands.takeInventoryGate(); gate != nil {
		if err := gate.wait(ctx); err != nil {
			return "", err
		}
	}
	return commands.delegate.ListComposeRunners(ctx)
}

func (commands *readinessDockerCommands) RemoveComposeRunner(ctx context.Context, containerID string) error {
	return commands.delegate.RemoveComposeRunner(ctx, containerID)
}

func (commands *readinessDockerCommands) ListComposeServices(
	ctx context.Context,
	configured dockerprovider.ComposeConfig,
) (string, error) {
	if gate := commands.takeValidationGate(); gate != nil {
		if err := gate.wait(ctx); err != nil {
			return "", err
		}
	}
	return commands.delegate.ListComposeServices(ctx, configured)
}

var _ dockerprovider.CommandRunner = (*readinessDockerCommands)(nil)
