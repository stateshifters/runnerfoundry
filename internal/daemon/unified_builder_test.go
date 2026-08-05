package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
	"github.com/meza/RunnerFoundry/internal/state"
)

func daemonTestID(seed byte) core.JobID {
	var identifier core.JobID
	identifier[15] = seed
	return identifier
}

func TestBuildUnifiedGenerationRunsConfiguredRuntime(t *testing.T) {
	t.Parallel()

	store := newBuilderStoreFake()
	manager := newBuilderManager(t, store)
	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation, err := buildUnifiedGeneration(builderConfig(), unifiedBuilderDependencies{
		store: store, manager: manager, member: daemonTestID(1),
		providers:  map[core.RunnerPool]execution.ExecutionProvider{"linux": builderProviderFake{}},
		leadership: permanentLeadership{}, currentTime: builderTime, memberTTL: time.Minute,
		ticker: ticker, processGate: &localLeadershipGate{},
	})
	if err != nil {
		t.Fatalf("build generation: %v", err)
	}
	if err = generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	<-store.capacityPublished
	if err = generation.Stop(context.Background()); err != nil {
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
	invalidTimeout := builderConfig()
	invalidTimeout.ScaleSets[0].JobStartTimeout = "bad"
	assertBuildRejected(t, config.Config{GitHub: config.GitHubConfig{DiscoveryInterval: "1m"}}, valid)
	assertBuildRejected(t, builderConfig(), missingProvider)
	assertBuildRejected(t, invalidTimeout, valid)
}

func validBuilderDependencies(t *testing.T) unifiedBuilderDependencies {
	t.Helper()
	store := newBuilderStoreFake()
	return unifiedBuilderDependencies{
		store: store, manager: newBuilderManager(t, store), member: daemonTestID(1),
		providers:  map[core.RunnerPool]execution.ExecutionProvider{"linux": builderProviderFake{}},
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
	orchestrator := runtimeengine.NewOrchestrator(store, manager.Source(), builderProviderFake{}, daemonTestID(1), builderTime)
	assertLifecycleAdapterFailures(t, orchestrator, wantErr)
	assertPoolAdapterFailures(t, orchestrator, wantErr)
	store.operationErr = nil
	assertLifecycleAdapterSuccess(t, orchestrator)
	assertPoolAdapterSuccess(t, orchestrator)
	closers := configuredClosers(unifiedBuilderDependencies{
		leadership: newExclusiveLeadership(errors.New("not leader"), nil),
		closers:    []runtimeCloser{closerFake{events: &[]string{}, name: "state"}},
	})
	if len(closers) != 2 {
		t.Fatalf("closers = %d, want 2", len(closers))
	}
}

func assertLifecycleAdapterSuccess(t *testing.T, orchestrator *runtimeengine.Orchestrator) {
	t.Helper()
	lifecycle := orchestrationLifecycle{orchestrator: orchestrator, batchSize: 1}
	if err := lifecycle.RecoverInterruptedAcquisitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.RetryPendingResults(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func assertPoolAdapterSuccess(t *testing.T, orchestrator *runtimeengine.Orchestrator) {
	t.Helper()
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(orchestrator, "linux", 1), orchestrator: orchestrator}
	if err := pool.RequestWork(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if err := pool.StartAcquired(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if err := pool.FailUnstarted(context.Background(), "linux", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := pool.Reconcile(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
}

func assertLifecycleAdapterFailures(t *testing.T, orchestrator *runtimeengine.Orchestrator, wantErr error) {
	t.Helper()
	lifecycle := orchestrationLifecycle{orchestrator: orchestrator, batchSize: 1}
	if err := lifecycle.RecoverInterruptedAcquisitions(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("recovery error = %v", err)
	}
	if err := lifecycle.RetryPendingResults(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("retry error = %v", err)
	}
}

func assertPoolAdapterFailures(t *testing.T, orchestrator *runtimeengine.Orchestrator, wantErr error) {
	t.Helper()
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(orchestrator, "linux", 1), orchestrator: orchestrator}
	if err := pool.RequestWork(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("request error = %v", err)
	}
	if err := pool.StartAcquired(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v", err)
	}
	if err := pool.FailUnstarted(context.Background(), "linux", time.Minute); !errors.Is(err, wantErr) {
		t.Fatalf("timeout error = %v", err)
	}
	if err := pool.Reconcile(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("reconcile error = %v", err)
	}
}

func builderConfig() config.Config {
	return config.Config{
		GitHub: config.GitHubConfig{DiscoveryInterval: "1m"},
		ScaleSets: []config.ScaleSetConfig{{
			Name: "linux", RunnerGroup: "default", MaxRunners: 1, JobStartTimeout: "1m",
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
func (builderProviderFake) Inspect(context.Context, string) ([]execution.RunnerInstance, error) {
	return nil, nil
}
func (builderProviderFake) Destroy(context.Context, execution.RunnerInstance) error { return nil }

type builderStoreFake struct {
	heartbeat         chan struct{}
	capacityPublished chan struct{}
	heartbeats        int
	capacities        int
	registrations     []state.Registration
	operationErr      error
}

func newBuilderStoreFake() *builderStoreFake {
	return &builderStoreFake{
		heartbeat: make(chan struct{}, 1), capacityPublished: make(chan struct{}, 1),
	}
}

func (*builderStoreFake) Enqueue(context.Context, core.JobRequest) (core.JobRecord, bool, error) {
	return core.JobRecord{}, false, nil
}
func (*builderStoreFake) ClaimOldest(context.Context, core.RunnerPool, core.MemberID, time.Time) (core.JobRecord, bool, error) {
	return core.JobRecord{}, false, nil
}
func (*builderStoreFake) MarkStartedBySource(context.Context, core.SourceReference, time.Time) (core.JobRecord, error) {
	return core.JobRecord{}, nil
}
func (*builderStoreFake) BeginAcquisition(context.Context, core.JobID, time.Time) (core.JobRecord, error) {
	return core.JobRecord{}, nil
}
func (*builderStoreFake) ConfirmAcquisition(context.Context, core.JobID, time.Time) (core.JobRecord, error) {
	return core.JobRecord{}, nil
}
func (*builderStoreFake) SetResultPending(context.Context, core.JobID, core.Result) (core.JobRecord, error) {
	return core.JobRecord{}, nil
}
func (*builderStoreFake) SetResultPendingBySource(context.Context, core.SourceReference, core.Result) (core.JobRecord, error) {
	return core.JobRecord{}, nil
}
func (*builderStoreFake) Finalize(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
	return core.Receipt{}, nil
}
func (*builderStoreFake) LookupReceipt(context.Context, core.SourceReference) (core.Receipt, bool, error) {
	return core.Receipt{}, false, nil
}
func (*builderStoreFake) SetMemberAttemptsResultPending(context.Context, core.MemberID, core.Result) ([]core.JobRecord, error) {
	return nil, nil
}
func (*builderStoreFake) Lookup(context.Context, core.JobID) (core.JobRecord, bool, error) {
	return core.JobRecord{}, false, nil
}

func (store *builderStoreFake) HeartbeatMember(context.Context, core.MemberID, time.Time) error {
	store.heartbeats++
	store.heartbeat <- struct{}{}
	return nil
}
func (store *builderStoreFake) PublishRunnerPoolCapacity(context.Context, core.MemberID, core.RunnerPool, int) error {
	store.capacities++
	store.capacityPublished <- struct{}{}
	return nil
}
func (*builderStoreFake) ListExpiredMembers(context.Context, time.Time) ([]core.MemberID, error) {
	return nil, nil
}
func (*builderStoreFake) CountLiveMembers(context.Context, time.Time) (int, error) { return 1, nil }
func (*builderStoreFake) ExpireMember(context.Context, core.MemberID) error        { return nil }
func (*builderStoreFake) AggregateRunnerPoolCapacity(context.Context, core.RunnerPool, time.Time) (int, error) {
	return 1, nil
}
func (store *builderStoreFake) UpsertRegistration(_ context.Context, registration state.Registration) error {
	store.registrations = append(store.registrations, registration)
	return nil
}
func (*builderStoreFake) Registration(context.Context, string, core.RunnerPool) (state.Registration, bool, error) {
	return state.Registration{}, false, nil
}
func (store *builderStoreFake) Registrations(context.Context) ([]state.Registration, error) {
	return append([]state.Registration(nil), store.registrations...), nil
}
func (*builderStoreFake) DeleteRegistration(context.Context, string, core.RunnerPool) error {
	return nil
}

func (store *builderStoreFake) PendingAcquisitions(ctx context.Context, limit int) ([]core.JobRecord, error) {
	if store.operationErr != nil {
		return nil, store.operationErr
	}
	return nil, nil
}
func (store *builderStoreFake) PendingResults(ctx context.Context, limit int) ([]core.JobRecord, error) {
	if store.operationErr != nil {
		return nil, store.operationErr
	}
	return nil, nil
}
func (store *builderStoreFake) AttemptsByMember(ctx context.Context, member core.MemberID, pool core.RunnerPool) ([]core.JobRecord, error) {
	if store.operationErr != nil {
		return nil, store.operationErr
	}
	return nil, nil
}
func (store *builderStoreFake) SetUnstartedAttemptsResultPending(ctx context.Context, pool core.RunnerPool, before time.Time, result core.Result) ([]core.JobRecord, error) {
	if store.operationErr != nil {
		return nil, store.operationErr
	}
	return nil, nil
}
