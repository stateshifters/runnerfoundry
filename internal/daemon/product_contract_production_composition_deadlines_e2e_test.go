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
	runtimeservice "github.com/stateshifters/runnerfoundry/internal/runtime"
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
	configureProductionCompositionDockerFactories(&dependencies, dockerCommands)
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
	fixture := productionCompositionDeadlineFixture{
		inventory: inventory, dockerProvider: dockerProvider, libvirtProvider: libvirtProvider,
		dockerCommands: dockerCommands, libvirtCommands: libvirtCommands,
	}
	assertProductionCompositionDefaultDeadlines(t, fixture)
	assertProductionCompositionEarlierCallerDeadlines(t, fixture)
}

func TestProductContractProductionCompositionProviderCommandsHonorCallerCancellation(t *testing.T) {
	configured := loadProductionCompositionConfig(t)
	dockerCommands := newCancellationBlockingDockerCommands()
	libvirtCommands := newCancellationBlockingLibvirtCommands()
	dependencies := newProductionDependencies()
	dependencies.newDockerLauncher = func(
		configured dockerprovider.ComposeConfig,
		memberID core.MemberID,
	) execution.RunnerLauncher {
		return dockerCommands.newLauncher(configured, memberID)
	}
	dependencies.newDockerInventory = func(memberID core.MemberID) execution.RunnerInventory {
		return dockerCommands.newInventory(memberID)
	}
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
	dependencies.newDockerLauncher = func(
		configured dockerprovider.ComposeConfig,
		memberID core.MemberID,
	) execution.RunnerLauncher {
		return dockerCommands.newLauncher(configured, memberID)
	}
	dependencies.newDockerInventory = func(memberID core.MemberID) execution.RunnerInventory {
		return dockerCommands.newInventory(memberID)
	}
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

func configureProductionCompositionDockerFactories(
	dependencies *productionDependencies,
	commands *productionCompositionDockerCommands,
) {
	dependencies.newDockerLauncher = func(
		configured dockerprovider.ComposeConfig,
		memberID core.MemberID,
	) execution.RunnerLauncher {
		return commands.newLauncher(configured, memberID)
	}
	dependencies.newDockerInventory = func(memberID core.MemberID) execution.RunnerInventory {
		return commands.newInventory(memberID)
	}
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
) cancellationBlockingDockerLauncher {
	return cancellationBlockingDockerLauncher{
		productionCompositionDockerLauncher: commands.productionCompositionDockerCommands.newLauncher(configured, memberID),
		entered:                             commands.entered,
	}
}

type cancellationBlockingDockerLauncher struct {
	productionCompositionDockerLauncher
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

type productionCompositionDeadlineFixture struct {
	inventory       runtimeservice.AggregateRunnerInventory
	dockerProvider  execution.RunnerLauncher
	libvirtProvider execution.RunnerLauncher
	dockerCommands  *productionCompositionDockerCommands
	libvirtCommands *productionCompositionLibvirtCommands
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
	fixture productionCompositionDeadlineFixture,
) {
	t.Helper()
	assertProductionCompositionTimeout(t, "Docker validation", fixture.dockerCommands.contexts, "validate",
		productionCompositionOperationDeadline)
	assertProductionCompositionTimeout(t, "libvirt validation", fixture.libvirtCommands.contexts, "validate",
		productionCompositionOperationDeadline)

	requireProductionCompositionOperation(t, "refresh aggregate inventory", fixture.inventory.Refresh(context.Background()))
	assertProductionCompositionInventoryTimeouts(t, fixture.dockerCommands, fixture.libvirtCommands,
		productionCompositionOperationDeadline)
	requireProductionCompositionInspection(
		context.Background(), t, "inspect Docker inventory", fixture.inventory, fixture.dockerProvider.EndpointID(),
	)
	requireProductionCompositionInspection(
		context.Background(), t, "inspect libvirt inventory", fixture.inventory, fixture.libvirtProvider.EndpointID(),
	)
	assertProductionCompositionInventoryTimeouts(t, fixture.dockerCommands, fixture.libvirtCommands,
		productionCompositionOperationDeadline)

	startAndDestroyProductionCompositionRunners(
		context.Background(), t, fixture, "default",
		func(description string, observations <-chan productionCompositionCommandContext, operation string, timeout time.Duration) {
			assertProductionCompositionTimeout(t, description, observations, operation, timeout)
		},
	)
}

func assertProductionCompositionEarlierCallerDeadlines(
	t *testing.T,
	fixture productionCompositionDeadlineFixture,
) {
	t.Helper()
	callerDeadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	requireProductionCompositionOperation(t, "validate Docker with caller deadline", fixture.dockerProvider.Validate(ctx))
	requireProductionCompositionOperation(t, "validate libvirt with caller deadline", fixture.libvirtProvider.Validate(ctx))
	assertProductionCompositionCallerDeadline(t, "Docker validation", fixture.dockerCommands.contexts, "validate", callerDeadline)
	assertProductionCompositionCallerDeadline(t, "libvirt validation", fixture.libvirtCommands.contexts, "validate", callerDeadline)
	requireProductionCompositionOperation(t, "refresh aggregate inventory with caller deadline", fixture.inventory.Refresh(ctx))
	assertProductionCompositionInventoryCallerDeadlines(t, fixture.dockerCommands, fixture.libvirtCommands, callerDeadline)
	requireProductionCompositionInspection(ctx, t, "inspect Docker with caller deadline", fixture.inventory, fixture.dockerProvider.EndpointID())
	requireProductionCompositionInspection(ctx, t, "inspect libvirt with caller deadline", fixture.inventory, fixture.libvirtProvider.EndpointID())
	assertProductionCompositionInventoryCallerDeadlines(t, fixture.dockerCommands, fixture.libvirtCommands, callerDeadline)

	startAndDestroyProductionCompositionRunners(
		ctx, t, fixture, "caller",
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
	fixture productionCompositionDeadlineFixture,
	suffix string,
	assertDeadline productionCompositionDeadlineAssertion,
) {
	t.Helper()
	dockerRunner := core.RunnerName("deadline-docker-" + suffix)
	libvirtRunner := core.RunnerName("deadline-libvirt-" + suffix)
	requireProductionCompositionStart(ctx, t, fixture.dockerProvider, "docker-pool", dockerRunner)
	requireProductionCompositionStart(ctx, t, fixture.libvirtProvider, "libvirt-pool", libvirtRunner)
	assertDeadline("Docker start", fixture.dockerCommands.contexts, "start", productionCompositionStartDeadline)
	assertDeadline("libvirt start", fixture.libvirtCommands.contexts, "start", productionCompositionStartDeadline)

	dockerInstance := requireProductionCompositionEndpointInstance(ctx, t, fixture.inventory, fixture.dockerProvider.EndpointID(), dockerRunner)
	assertDeadline("Docker inventory before destroy", fixture.dockerCommands.contexts, "inventory", productionCompositionOperationDeadline)
	libvirtInstance := requireProductionCompositionEndpointInstance(ctx, t, fixture.inventory, fixture.libvirtProvider.EndpointID(), libvirtRunner)
	assertDeadline("libvirt inventory before destroy", fixture.libvirtCommands.contexts, "inventory", productionCompositionOperationDeadline)
	assertDeadline("libvirt XML inventory before destroy", fixture.libvirtCommands.contexts, "inventory", productionCompositionOperationDeadline)
	assertDeadline("libvirt state inventory before destroy", fixture.libvirtCommands.contexts, "inventory", productionCompositionOperationDeadline)
	requireProductionCompositionDestroy(ctx, t, fixture.inventory, dockerInstance)
	requireProductionCompositionDestroy(ctx, t, fixture.inventory, libvirtInstance)
	assertDeadline("Docker destroy", fixture.dockerCommands.contexts, "destroy", productionCompositionOperationDeadline)
	assertDeadline("libvirt destroy", fixture.libvirtCommands.contexts, "destroy", productionCompositionOperationDeadline)
}

func requireProductionCompositionInspection(
	ctx context.Context,
	t *testing.T,
	description string,
	inventory runtimeservice.AggregateRunnerInventory,
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
	inventory runtimeservice.AggregateRunnerInventory,
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
	inventory runtimeservice.AggregateRunnerInventory,
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
