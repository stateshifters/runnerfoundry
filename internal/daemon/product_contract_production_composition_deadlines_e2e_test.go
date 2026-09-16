//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
)

const (
	productionCompositionOperationDeadline = 30 * time.Second
	productionCompositionStartDeadline     = 5 * time.Minute
)

func TestProductContractProductionCompositionAppliesProviderDeadlines(t *testing.T) {
	configured := loadProductionCompositionConfig(t)
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	dependencies := newProductionDependencies()
	dependencies.newDockerLauncher = dockerCommands.newLauncher
	dependencies.newDockerInventory = dockerCommands.newInventory
	dependencies.libvirtCommands = libvirtCommands
	dependencies.readFile = productionCompositionFileReader(t)

	providers, inventory, err := buildProductionProvidersWithDependencies(
		context.Background(), configured, core.MemberID("production-member"), dependencies,
	)
	if err != nil {
		t.Fatalf("build production-composed providers: %v", err)
	}
	dockerProvider, dockerExists := providers["docker-pool"]
	libvirtProvider, libvirtExists := providers["libvirt-pool"]
	if !dockerExists || !libvirtExists {
		t.Fatalf("production-composed providers = %v, want Docker and libvirt pools", providers)
	}

	assertProductionCompositionDefaultDeadlines(
		t, inventory, dockerProvider, libvirtProvider, dockerCommands, libvirtCommands,
	)
	assertProductionCompositionEarlierCallerDeadlines(
		t, inventory, dockerProvider, libvirtProvider, dockerCommands, libvirtCommands,
	)
}

func TestProductContractProductionCompositionProviderCommandsHonorCallerCancellation(t *testing.T) {
	configured := loadProductionCompositionConfig(t)
	dockerCommands := newCancellationBlockingDockerCommands()
	libvirtCommands := newCancellationBlockingLibvirtCommands()
	dependencies := newProductionDependencies()
	dependencies.newDockerLauncher = dockerCommands.newLauncher
	dependencies.newDockerInventory = dockerCommands.newInventory
	dependencies.libvirtCommands = libvirtCommands
	dependencies.readFile = productionCompositionFileReader(t)

	providers, _, err := buildProductionProvidersWithDependencies(
		context.Background(), configured, core.MemberID("production-member"), dependencies,
	)
	if err != nil {
		t.Fatalf("build production-composed providers: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dockerResult := startProductionCompositionRunner(ctx, providers["docker-pool"], "docker-pool", "cancel-docker")
	libvirtResult := startProductionCompositionRunner(ctx, providers["libvirt-pool"], "libvirt-pool", "cancel-libvirt")
	awaitProductionCompositionCommandEntry(t, "Docker start", dockerCommands.entered)
	awaitProductionCompositionCommandEntry(t, "libvirt start", libvirtCommands.entered)
	cancel()
	assertProductionCompositionCancellation(t, "Docker start", dockerResult, context.Canceled)
	assertProductionCompositionCancellation(t, "libvirt start", libvirtResult, context.Canceled)
}

func TestProductContractProductionCompositionProviderCommandsHonorEarlierCallerDeadline(t *testing.T) {
	configured := loadProductionCompositionConfig(t)
	dockerCommands := newCancellationBlockingDockerCommands()
	libvirtCommands := newCancellationBlockingLibvirtCommands()
	dependencies := newProductionDependencies()
	dependencies.newDockerLauncher = dockerCommands.newLauncher
	dependencies.newDockerInventory = dockerCommands.newInventory
	dependencies.libvirtCommands = libvirtCommands
	dependencies.readFile = productionCompositionFileReader(t)

	providers, _, err := buildProductionProvidersWithDependencies(
		context.Background(), configured, core.MemberID("production-member"), dependencies,
	)
	if err != nil {
		t.Fatalf("build production-composed providers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dockerResult := startProductionCompositionRunner(ctx, providers["docker-pool"], "docker-pool", "deadline-docker")
	libvirtResult := startProductionCompositionRunner(ctx, providers["libvirt-pool"], "libvirt-pool", "deadline-libvirt")
	awaitProductionCompositionCommandEntry(t, "Docker start", dockerCommands.entered)
	awaitProductionCompositionCommandEntry(t, "libvirt start", libvirtCommands.entered)
	assertProductionCompositionCancellation(t, "Docker start", dockerResult, context.DeadlineExceeded)
	assertProductionCompositionCancellation(t, "libvirt start", libvirtResult, context.DeadlineExceeded)
}

func startProductionCompositionRunner(
	ctx context.Context,
	provider execution.RunnerLauncher,
	runnerPool core.RunnerPool,
	runnerName core.RunnerName,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- provider.Start(ctx, execution.StartRequest{
			JobID: uuid.New(), RunnerPool: runnerPool, EndpointID: provider.EndpointID(), RunnerName: runnerName,
			Bootstrap: core.NewBootstrap([]byte("opaque-bootstrap")),
		})
	}()
	return result
}

func awaitProductionCompositionCommandEntry(t *testing.T, description string, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(e2eWaitLimit):
		t.Fatalf("timed out awaiting blocked %s command", description)
	}
}

func assertProductionCompositionCancellation(
	t *testing.T,
	description string,
	result <-chan error,
	want error,
) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("%s error = %v, want %v", description, err, want)
		}
	case <-time.After(e2eWaitLimit):
		t.Fatalf("timed out awaiting canceled %s command", description)
	}
}

type cancellationBlockingDockerCommands struct {
	*productionCompositionDockerCommands
	entered chan struct{}
}

func newCancellationBlockingDockerCommands() *cancellationBlockingDockerCommands {
	return &cancellationBlockingDockerCommands{
		productionCompositionDockerCommands: newProductionCompositionDockerCommands(),
		entered:                             make(chan struct{}, 1),
	}
}

func (commands *cancellationBlockingDockerCommands) newLauncher(
	configured dockerprovider.ComposeConfig,
	memberID core.MemberID,
) execution.RunnerLauncher {
	return cancellationBlockingDockerLauncher{
		RunnerLauncher: commands.productionCompositionDockerCommands.newLauncher(configured, memberID),
		entered:        commands.entered,
	}
}

type cancellationBlockingDockerLauncher struct {
	execution.RunnerLauncher
	entered chan<- struct{}
}

func (launcher cancellationBlockingDockerLauncher) Start(ctx context.Context, _ execution.StartRequest) error {
	launcher.entered <- struct{}{}
	<-ctx.Done()
	return fmt.Errorf("blocked scripted Docker start: %w", ctx.Err())
}

type cancellationBlockingLibvirtCommands struct {
	*productionCompositionLibvirtCommands
	entered chan struct{}
}

func newCancellationBlockingLibvirtCommands() *cancellationBlockingLibvirtCommands {
	return &cancellationBlockingLibvirtCommands{
		productionCompositionLibvirtCommands: newProductionCompositionLibvirtCommands(),
		entered:                              make(chan struct{}, 1),
	}
}

func (commands *cancellationBlockingLibvirtCommands) CreateDomain(ctx context.Context, _, _ string) error {
	commands.entered <- struct{}{}
	<-ctx.Done()
	return fmt.Errorf("blocked scripted libvirt start: %w", ctx.Err())
}

func assertProductionCompositionDefaultDeadlines(
	t *testing.T,
	inventory execution.AggregateRunnerInventory,
	dockerProvider, libvirtProvider execution.RunnerLauncher,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
) {
	t.Helper()
	assertProductionCompositionTimeout(t, "Docker validation", dockerCommands.contexts, "validate",
		productionCompositionOperationDeadline)
	assertProductionCompositionTimeout(t, "libvirt validation", libvirtCommands.contexts, "validate",
		productionCompositionOperationDeadline)

	requireProductionCompositionOperation(t, "refresh aggregate inventory", inventory.Refresh(context.Background()))
	assertProductionCompositionInventoryTimeouts(t, dockerCommands, libvirtCommands,
		productionCompositionOperationDeadline)
	requireProductionCompositionInspection(
		context.Background(), t, "inspect Docker inventory", inventory, dockerProvider.EndpointID(),
	)
	requireProductionCompositionInspection(
		context.Background(), t, "inspect libvirt inventory", inventory, libvirtProvider.EndpointID(),
	)
	assertProductionCompositionInventoryTimeouts(t, dockerCommands, libvirtCommands,
		productionCompositionOperationDeadline)

	startAndDestroyProductionCompositionRunners(
		context.Background(), t, inventory, dockerProvider, libvirtProvider, dockerCommands, libvirtCommands, "default",
		func(description string, observations <-chan productionCompositionCommandContext, operation string, timeout time.Duration) {
			assertProductionCompositionTimeout(t, description, observations, operation, timeout)
		},
	)
}

func assertProductionCompositionEarlierCallerDeadlines(
	t *testing.T,
	inventory execution.AggregateRunnerInventory,
	dockerProvider, libvirtProvider execution.RunnerLauncher,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
) {
	t.Helper()
	callerDeadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	requireProductionCompositionOperation(t, "validate Docker with caller deadline", dockerProvider.Validate(ctx))
	requireProductionCompositionOperation(t, "validate libvirt with caller deadline", libvirtProvider.Validate(ctx))
	assertProductionCompositionCallerDeadline(t, "Docker validation", dockerCommands.contexts, "validate", callerDeadline)
	assertProductionCompositionCallerDeadline(t, "libvirt validation", libvirtCommands.contexts, "validate", callerDeadline)
	requireProductionCompositionOperation(t, "refresh aggregate inventory with caller deadline", inventory.Refresh(ctx))
	assertProductionCompositionInventoryCallerDeadlines(t, dockerCommands, libvirtCommands, callerDeadline)
	requireProductionCompositionInspection(ctx, t, "inspect Docker with caller deadline", inventory, dockerProvider.EndpointID())
	requireProductionCompositionInspection(ctx, t, "inspect libvirt with caller deadline", inventory, libvirtProvider.EndpointID())
	assertProductionCompositionInventoryCallerDeadlines(t, dockerCommands, libvirtCommands, callerDeadline)

	startAndDestroyProductionCompositionRunners(
		ctx, t, inventory, dockerProvider, libvirtProvider, dockerCommands, libvirtCommands, "caller",
		func(description string, observations <-chan productionCompositionCommandContext, operation string, _ time.Duration) {
			assertProductionCompositionCallerDeadline(t, description, observations, operation, callerDeadline)
		},
	)
}

type productionCompositionDeadlineAssertion func(
	description string,
	observations <-chan productionCompositionCommandContext,
	operation string,
	timeout time.Duration,
)

func startAndDestroyProductionCompositionRunners(
	ctx context.Context,
	t *testing.T,
	inventory execution.AggregateRunnerInventory,
	dockerProvider, libvirtProvider execution.RunnerLauncher,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	suffix string,
	assertDeadline productionCompositionDeadlineAssertion,
) {
	t.Helper()
	dockerRunner := core.RunnerName("deadline-docker-" + suffix)
	libvirtRunner := core.RunnerName("deadline-libvirt-" + suffix)
	requireProductionCompositionStart(ctx, t, dockerProvider, "docker-pool", dockerRunner)
	requireProductionCompositionStart(ctx, t, libvirtProvider, "libvirt-pool", libvirtRunner)
	assertDeadline("Docker start", dockerCommands.contexts, "start", productionCompositionStartDeadline)
	assertDeadline("libvirt start", libvirtCommands.contexts, "start", productionCompositionStartDeadline)

	dockerInstance := requireProductionCompositionEndpointInstance(ctx, t, inventory, dockerProvider.EndpointID(), dockerRunner)
	assertDeadline("Docker inventory before destroy", dockerCommands.contexts, "inventory", productionCompositionOperationDeadline)
	libvirtInstance := requireProductionCompositionEndpointInstance(ctx, t, inventory, libvirtProvider.EndpointID(), libvirtRunner)
	assertDeadline("libvirt inventory before destroy", libvirtCommands.contexts, "inventory", productionCompositionOperationDeadline)
	assertDeadline("libvirt XML inventory before destroy", libvirtCommands.contexts, "inventory", productionCompositionOperationDeadline)
	assertDeadline("libvirt state inventory before destroy", libvirtCommands.contexts, "inventory", productionCompositionOperationDeadline)
	requireProductionCompositionDestroy(ctx, t, inventory, dockerInstance)
	requireProductionCompositionDestroy(ctx, t, inventory, libvirtInstance)
	assertDeadline("Docker destroy", dockerCommands.contexts, "destroy", productionCompositionOperationDeadline)
	assertDeadline("libvirt destroy", libvirtCommands.contexts, "destroy", productionCompositionOperationDeadline)
}

func requireProductionCompositionInspection(
	ctx context.Context,
	t *testing.T,
	description string,
	inventory execution.AggregateRunnerInventory,
	endpointID execution.InventoryEndpointID,
) {
	t.Helper()
	instances, err := inventory.InspectEndpoint(ctx, endpointID)
	if err != nil || len(instances) != 0 {
		t.Fatalf("%s = (%v, %v), want empty successful inventory", description, instances, err)
	}
}

func requireProductionCompositionStart(
	ctx context.Context,
	t *testing.T,
	provider execution.RunnerLauncher,
	runnerPool core.RunnerPool,
	runnerName core.RunnerName,
) {
	t.Helper()
	err := provider.Start(ctx, execution.StartRequest{
		JobID: uuid.New(), RunnerPool: runnerPool, EndpointID: provider.EndpointID(), RunnerName: runnerName,
		Bootstrap: core.NewBootstrap([]byte("opaque-bootstrap")),
	})
	requireProductionCompositionOperation(t, fmt.Sprintf("start %s", runnerName), err)
}

func requireProductionCompositionEndpointInstance(
	ctx context.Context,
	t *testing.T,
	inventory execution.AggregateRunnerInventory,
	endpointID execution.InventoryEndpointID,
	runnerName core.RunnerName,
) execution.RunnerInstance {
	t.Helper()
	instances, err := inventory.InspectEndpoint(ctx, endpointID)
	if err != nil {
		t.Fatalf("inspect endpoint before destroy %s: %v", runnerName, err)
	}
	for _, instance := range instances {
		if instance.RunnerName == runnerName {
			return instance
		}
	}
	t.Fatalf("runner %s was not observed before destroy", runnerName)
	return execution.RunnerInstance{}
}

func requireProductionCompositionDestroy(
	ctx context.Context,
	t *testing.T,
	inventory execution.AggregateRunnerInventory,
	instance execution.RunnerInstance,
) {
	t.Helper()
	err := inventory.Destroy(ctx, instance)
	requireProductionCompositionOperation(t, fmt.Sprintf("destroy %s", instance.RunnerName), err)
}

func requireProductionCompositionOperation(t *testing.T, description string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", description, err)
	}
}

func assertProductionCompositionInventoryTimeouts(
	t *testing.T,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	want time.Duration,
) {
	t.Helper()
	assertProductionCompositionTimeout(t, "Docker inventory", dockerCommands.contexts, "inventory", want)
	assertProductionCompositionTimeout(t, "libvirt inventory", libvirtCommands.contexts, "inventory", want)
}

func assertProductionCompositionInventoryCallerDeadlines(
	t *testing.T,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	want time.Time,
) {
	t.Helper()
	assertProductionCompositionCallerDeadline(t, "Docker inventory", dockerCommands.contexts, "inventory", want)
	assertProductionCompositionCallerDeadline(t, "libvirt inventory", libvirtCommands.contexts, "inventory", want)
}

func assertProductionCompositionTimeout(
	t *testing.T,
	description string,
	observations <-chan productionCompositionCommandContext,
	operation string,
	want time.Duration,
) {
	t.Helper()
	observation := awaitProductionCompositionCommandContext(t, description, observations, operation)
	remaining := observation.deadline.Sub(observation.observedAt)
	if !observation.hasDeadline || remaining > want || remaining < want-2*time.Second {
		t.Fatalf("%s deadline remaining = %s (present %t), want approximately %s",
			description, remaining, observation.hasDeadline, want)
	}
}

func assertProductionCompositionCallerDeadline(
	t *testing.T,
	description string,
	observations <-chan productionCompositionCommandContext,
	operation string,
	want time.Time,
) {
	t.Helper()
	observation := awaitProductionCompositionCommandContext(t, description, observations, operation)
	if !observation.hasDeadline || !observation.deadline.Equal(want) {
		t.Fatalf("%s deadline = %v (present %t), want caller deadline %v",
			description, observation.deadline, observation.hasDeadline, want)
	}
}

func awaitProductionCompositionCommandContext(
	t *testing.T,
	description string,
	observations <-chan productionCompositionCommandContext,
	operation string,
) productionCompositionCommandContext {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		select {
		case observation := <-observations:
			if observation.operation == operation {
				return observation
			}
			t.Fatalf("%s observed command operation %q, want %q", description, observation.operation, operation)
		case <-timer.C:
			t.Fatalf("timed out awaiting %s command context", description)
		}
	}
}
