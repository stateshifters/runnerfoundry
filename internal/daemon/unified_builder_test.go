package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func daemonTestID(seed byte) core.MemberID {
	return core.MemberID(fmt.Sprintf("runner-host-%d", seed))
}

func TestBuildUnifiedGenerationRunsConfiguredRuntime(t *testing.T) {
	t.Parallel()

	store := newBuilderStoreFake()
	manager := newBuilderManager(t, store)
	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation, err := buildUnifiedGeneration(builderConfig(), unifiedBuilderDependencies{
		store: store, manager: manager, member: daemonTestID(1),
		providers:  map[core.RunnerPool]execution.ExecutionProvider{"linux": builderProviderFake{}},
		inventory:  builderProviderFake{},
		leadership: permanentLeadership{}, currentTime: builderTime, memberTTL: time.Minute,
		ticker: ticker, processGate: &localLeadershipGate{},
	})
	if err != nil {
		t.Fatalf("build generation: %v", err)
	}
	if err = generation.Prepare(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err = generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	<-store.capacityPublished
	if err = generation.Close(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
	if store.capacities != 1 {
		t.Fatalf("published capacities = %d, want 1", store.capacities)
	}
}

func TestBuildUnifiedGenerationRejectsInvalidComposition(t *testing.T) {
	t.Parallel()
	valid := validBuilderDependencies(t)
	invalidTTL := valid
	invalidTTL.memberTTL = 0
	invalidDiscovery := builderConfig()
	invalidDiscovery.GitHub.DiscoveryInterval = "bad"
	assertBuildRejected(t, builderConfig(), unifiedBuilderDependencies{})
	assertBuildRejected(t, builderConfig(), invalidTTL)
	assertBuildRejected(t, invalidDiscovery, valid)
}

func TestBuildUnifiedGenerationRejectsInvalidRunnerPools(t *testing.T) {
	t.Parallel()
	valid := validBuilderDependencies(t)
	missingProvider := valid
	missingProvider.providers = nil
	assertBuildRejected(t, config.Config{GitHub: config.GitHubConfig{DiscoveryInterval: "1m"}}, valid)
	assertBuildRejected(t, builderConfig(), missingProvider)
}

func validBuilderDependencies(t *testing.T) unifiedBuilderDependencies {
	t.Helper()
	store := newBuilderStoreFake()
	return unifiedBuilderDependencies{
		store: store, manager: newBuilderManager(t, store), member: daemonTestID(1),
		providers:  map[core.RunnerPool]execution.ExecutionProvider{"linux": builderProviderFake{}},
		inventory:  builderProviderFake{},
		leadership: permanentLeadership{}, currentTime: builderTime, memberTTL: time.Minute,
		ticker: &manualCycleTicker{ticks: make(chan time.Time)}, processGate: &localLeadershipGate{},
	}
}

func assertBuildRejected(t *testing.T, configured config.Config, dependencies unifiedBuilderDependencies) {
	t.Helper()
	if _, err := buildUnifiedGeneration(configured, dependencies); err == nil {
		t.Fatal("invalid composition was accepted")
	}
}

func TestOrchestrationAdaptersPreserveRuntimeFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("state failed")
	store := newBuilderStoreFake()
	store.operationErr = wantErr
	manager := newBuilderManager(t, store)
	provider := builderProviderFake{}
	orchestrator := newBuilderOrchestrator(store, manager, provider)
	assertPoolAdapterFailures(t, orchestrator, wantErr)
	store.operationErr = nil
	assertPoolAdapterSuccess(t, orchestrator)
	pool := orchestrationPool{orchestrator: orchestrator, runnerPool: "linux"}
	if pool.RunnerPool() != "linux" {
		t.Fatalf("finalizer Runner Pool = %q", pool.RunnerPool())
	}
	if remaining, err := pool.Terminate(context.Background()); err != nil || remaining != 0 {
		t.Fatalf("clean up empty pool: %v", err)
	}
	store.operationErr = wantErr
	if _, err := pool.Terminate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("cleanup error = %v, want %v", err, wantErr)
	}
	closers := configuredClosers(unifiedBuilderDependencies{
		leadership: newExclusiveLeadership(errors.New("not leader"), nil),
		closers:    []runtimeCloser{closerFake{events: &[]string{}, name: "state"}},
	})
	if len(closers) != 2 {
		t.Fatalf("closers = %d, want 2", len(closers))
	}
}

func newBuilderOrchestrator(
	store *builderStoreFake,
	manager *githubsource.Manager,
	provider builderProviderFake,
) *runtimeengine.Orchestrator {
	return runtimeengine.NewOrchestrator(
		store, manager.Source(), provider, provider, daemonTestID(1), builderTime,
	)
}

func assertPoolAdapterSuccess(t *testing.T, orchestrator *runtimeengine.Orchestrator) {
	t.Helper()
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(orchestrator, "linux", 1), orchestrator: orchestrator}
	if err := pool.RequestWork(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if err := pool.Reconcile(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if err := pool.RecoverForeign(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
}

func assertPoolAdapterFailures(t *testing.T, orchestrator *runtimeengine.Orchestrator, wantErr error) {
	t.Helper()
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(orchestrator, "linux", 1), orchestrator: orchestrator}
	if err := pool.RequestWork(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("request error = %v", err)
	}
	if err := pool.Reconcile(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("reconcile error = %v", err)
	}
	if err := pool.RecoverForeign(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("foreign recovery error = %v", err)
	}
}

func builderConfig() config.Config {
	return config.Config{
		GitHub: config.GitHubConfig{DiscoveryInterval: "1m"},
		RunnerPools: []config.RunnerPoolConfig{{
			Name: "linux", RunnerGroup: "default", MaxRunners: 1,
		}},
	}
}

func builderTime() time.Time { return time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC) }

func newBuilderManager(t *testing.T, store *builderStoreFake) *githubsource.Manager {
	t.Helper()
	manager, err := githubsource.NewManager(
		builderDiscovererFake{},
		func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
			return githubsource.ManagerClient{}, errors.New("unexpected client request")
		},
		store,
		githubsource.ManagerObserver{},
	)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	return manager
}

type builderDiscovererFake struct{}

func (builderDiscovererFake) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{}, nil
}

type builderProviderFake struct{}

func (builderProviderFake) Validate(context.Context) error                      { return nil }
func (builderProviderFake) Start(context.Context, execution.StartRequest) error { return nil }
func (builderProviderFake) EndpointID() execution.InventoryEndpointID           { return "test:builder" }
func (provider builderProviderFake) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	return provider.EndpointID() == endpointID
}
func (provider builderProviderFake) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	_ core.MemberID,
) bool {
	return provider.KnowsEndpoint(endpointID)
}
func (provider builderProviderFake) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}
func (provider builderProviderFake) InspectEndpoint(ctx context.Context, _ execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	return provider.Inspect(ctx)
}
func (builderProviderFake) Inspect(context.Context) ([]execution.RunnerInstance, error) {
	return nil, nil
}
func (builderProviderFake) Destroy(context.Context, execution.RunnerInstance) error { return nil }

type builderStoreFake struct {
	heartbeat         chan struct{}
	capacityPublished chan struct{}
	heartbeats        int
	capacities        int
	registrations     []githubsource.Registration
	operationErr      error
}

func newBuilderStoreFake() *builderStoreFake {
	return &builderStoreFake{
		heartbeat: make(chan struct{}, 1), capacityPublished: make(chan struct{}, 1),
	}
}

func (*builderStoreFake) Queue(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{}, nil
}
func (store *builderStoreFake) ClaimOldest(_ context.Context, _ core.RunnerPool, _ core.ProcessingClaim) (core.ClaimResult, error) {
	return core.ClaimResult{Outcome: core.ClaimEmpty}, store.operationErr
}
func (*builderStoreFake) TerminalizeSource(context.Context, core.SourceReference) error { return nil }
func (*builderStoreFake) MarkRunnerStarted(context.Context, core.SourceReference) error { return nil }
func (store *builderStoreFake) ProcessingByMember(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
	return nil, store.operationErr
}
func (store *builderStoreFake) ProcessingByPool(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
	return nil, store.operationErr
}
func (*builderStoreFake) RetryUnstarted(context.Context, core.JobID) (core.RetryResult, error) {
	return core.RetryResult{Outcome: core.RetryMissing}, nil
}
func (*builderStoreFake) RecoverUnstartedProcessing(context.Context, core.ProcessingJob) (core.RetryResult, error) {
	return core.RetryResult{Outcome: core.RetryMissing}, nil
}
func (*builderStoreFake) RemoveTerminal(context.Context, core.JobID) (core.TerminalRemovalResult, error) {
	return core.TerminalRemovalResult{Outcome: core.TerminalMissing}, nil
}

func (store *builderStoreFake) PublishMemberState(
	context.Context,
	core.MemberID,
	time.Time,
	[]core.RunnerPoolCapacity,
) error {
	store.heartbeats++
	store.capacities++
	store.heartbeat <- struct{}{}
	store.capacityPublished <- struct{}{}
	return nil
}
func (*builderStoreFake) CountLiveMembers(context.Context, time.Time) (int, error) { return 1, nil }
func (*builderStoreFake) WithdrawMemberCapacity(context.Context, core.MemberID) error {
	return nil
}
func (*builderStoreFake) AvailableRunnerPoolCapacity(context.Context, core.RunnerPool, time.Time) (int, error) {
	return 1, nil
}
func (store *builderStoreFake) UpsertRegistration(_ context.Context, registration githubsource.Registration) error {
	store.registrations = append(store.registrations, registration)
	return nil
}
func (store *builderStoreFake) ReplaceRegistrations(_ context.Context, registrations []githubsource.Registration) error {
	store.registrations = append([]githubsource.Registration(nil), registrations...)
	return nil
}
func (store *builderStoreFake) ReplaceRegistrationSnapshot(
	_ context.Context,
	snapshot githubsource.RegistrationSnapshot,
) error {
	store.registrations = append([]githubsource.Registration(nil), snapshot.Active...)
	return nil
}
func (*builderStoreFake) Registration(context.Context, string, core.RunnerPool) (githubsource.Registration, bool, error) {
	return githubsource.Registration{}, false, nil
}
func (store *builderStoreFake) Registrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), store.registrations...), nil
}
func (store *builderStoreFake) RegistrationSnapshot(context.Context) (githubsource.RegistrationSnapshot, error) {
	registrations := append([]githubsource.Registration(nil), store.registrations...)
	return githubsource.RegistrationSnapshot{
		Active: registrations,
		Owned:  append([]githubsource.Registration(nil), registrations...),
	}, nil
}
func (*builderStoreFake) DeleteRegistration(context.Context, string, core.RunnerPool) error {
	return nil
}
