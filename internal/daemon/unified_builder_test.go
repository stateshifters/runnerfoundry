package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func builderStatePorts(store *builderStoreFake) runtimeStatePorts {
	return runtimeStatePorts{jobs: store, processing: store, transitions: store,
		provisioningFailures: store, members: store, capacity: store}
}

func daemonTestID(seed byte) core.MemberID {
	return core.MemberID(fmt.Sprintf("runner-host-%d", seed))
}

func TestBuildUnifiedGenerationRunsConfiguredRuntime(t *testing.T) {
	t.Parallel()

	store := newBuilderStoreFake()
	manager := newBuilderManager(t, store)
	generation, err := buildUnifiedGeneration(builderConfig(), unifiedBuilderDependencies{
		state: builderStatePorts(store), demandStore: store, manager: manager, member: daemonTestID(1),
		providers:  map[core.RunnerPool]execution.RunnerLauncher{"linux": builderProviderFake{}},
		inventory:  builderProviderFake{},
		leadership: permanentLeadership{}, currentTime: builderTime, memberTTL: time.Minute,
		tickers: newManualMaintenanceLaneTickers(), processGate: &localLeadershipGate{},
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

func TestBuildUnifiedGenerationRejectsMissingSourceOperations(t *testing.T) {
	t.Parallel()
	dependencies := validBuilderDependencies(t)
	dependencies.manager = sourceLessManager{githubManagerComposition: dependencies.manager}
	assertBuildRejected(t, builderConfig(), dependencies)
}

type sourceLessManager struct{ githubManagerComposition }

func (sourceLessManager) Source() *githubsource.Source { return nil }

func TestBuildUnifiedGenerationAttachesPostgreSQLQueueNotifications(t *testing.T) {
	configured := builderConfig()
	configured.State.Postgres = &postgresconfig.Settings{Role: "runnerfoundry"}
	dependencies := validBuilderDependencies(t)
	reported := make(chan error, 1)
	dependencies.report = func(err error) { reported <- err }
	notificationPool := queueNotificationPool{maxConnections: 2}
	dependencies.queueNotifications = &notificationPool
	generation, err := buildUnifiedGeneration(configured, dependencies)
	if err != nil {
		t.Fatalf("build PostgreSQL generation: %v", err)
	}
	if generation.notifications == nil {
		t.Fatal("PostgreSQL generation has no queue notification listener")
	}
	listener, ok := generation.notifications.(*postgreSQLQueueNotificationListener)
	if !ok {
		t.Fatalf("notification runtime = %T, want PostgreSQL listener", generation.notifications)
	}
	listener.report(errors.New("listener failed"))
	var envelope operationalEventEnvelope
	if err := <-reported; !errors.As(err, &envelope) || envelope.event.operation != "queue_notifications" {
		t.Fatalf("listener report = %v, want queue notification operational event", err)
	}
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
		state: builderStatePorts(store), demandStore: store, manager: newBuilderManager(t, store), member: daemonTestID(1),
		providers:  map[core.RunnerPool]execution.RunnerLauncher{"linux": builderProviderFake{}},
		inventory:  builderProviderFake{},
		leadership: permanentLeadership{}, currentTime: builderTime, memberTTL: time.Minute,
		tickers: newManualMaintenanceLaneTickers(), processGate: &localLeadershipGate{},
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
	orchestrator := newBuilderOrchestrator(t, store, manager, provider)
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
	if remaining, err := pool.Terminate(context.Background()); !errors.Is(err, wantErr) || remaining != 0 {
		t.Fatalf("cleanup failure = remaining:%d error:%v, want no completion result and error %v",
			remaining, err, wantErr)
	}
	closers := configuredClosers(unifiedBuilderDependencies{
		leadership: newExclusiveLeadership(errors.New("not leader"), nil),
		closers:    []runtimeCloser{closerFake{events: &[]string{}, name: "state"}},
	})
	if len(closers) != 2 {
		t.Fatalf("closers = %d, want 2", len(closers))
	}
}

func TestOrchestrationPoolDoesNotReportShutdownClaimCancellation(t *testing.T) {
	for _, test := range []struct {
		name       string
		claimErr   error
		contextErr error
		wantEvents int
	}{
		{name: "shutdown cancellation", claimErr: context.Canceled, contextErr: context.Canceled},
		{name: "joined shutdown cancellation", claimErr: errors.Join(context.Canceled, fmt.Errorf("claim interrupted: %w", context.Canceled)),
			contextErr: context.Canceled},
		{name: "independent cancellation", claimErr: context.Canceled, wantEvents: 1},
		{name: "storage failure", claimErr: errors.New("storage unavailable"), wantEvents: 1},
		{name: "storage failure during shutdown", claimErr: errors.New("storage unavailable"), contextErr: context.Canceled, wantEvents: 1},
		{name: "cancellation with storage failure", claimErr: errors.Join(context.Canceled, errors.New("rollback failed")),
			contextErr: context.Canceled, wantEvents: 1},
		{name: "deadline with canceled claim", claimErr: context.Canceled, contextErr: context.DeadlineExceeded, wantEvents: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertOrchestrationClaimReporting(t, test.claimErr, test.contextErr, test.wantEvents)
		})
	}
}

func assertOrchestrationClaimReporting(t *testing.T, claimErr, contextErr error, wantEvents int) {
	t.Helper()
	store := newBuilderStoreFake()
	store.operationErr = claimErr
	orchestrator := newBuilderOrchestrator(t, store, newBuilderManager(t, store), builderProviderFake{})
	reported := make([]operationalEvent, 0, 1)
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(orchestrator, "linux", 1),
		runnerPool: "linux", stateBackend: "sqlite", report: collectBuilderPoolEvents(t, &reported)}
	ctx, cancel := builderClaimContext(contextErr)
	defer cancel()
	if _, err := pool.RequestWork(ctx, "linux"); !errors.Is(err, claimErr) {
		t.Fatalf("request error = %v, want %v", err, claimErr)
	}
	if len(reported) != wantEvents {
		t.Fatalf("claim events = %+v, want %d", reported, wantEvents)
	}
	for _, event := range reported {
		if event.operation != "claim" || event.stage != "claim_oldest" || event.outcome != "failed" ||
			!errors.Is(event.err, claimErr) {
			t.Fatalf("claim failure event = %+v", event)
		}
	}
}

func builderClaimContext(contextErr error) (context.Context, context.CancelFunc) {
	if errors.Is(contextErr, context.DeadlineExceeded) {
		return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	}
	ctx, cancel := context.WithCancel(context.Background())
	if errors.Is(contextErr, context.Canceled) {
		cancel()
	}
	return ctx, cancel
}

func collectBuilderPoolEvents(t *testing.T, reported *[]operationalEvent) func(error) {
	t.Helper()
	return func(err error) {
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("report = %T, want operational event", err)
		}
		*reported = append(*reported, envelope.event)
	}
}

func TestOrchestrationPoolReportsClaimedOutcomeDuringCancellation(t *testing.T) {
	reported := make([]operationalEvent, 0, 2)
	claimErr := fmt.Errorf("cancel claimed job: %w", context.Canceled)
	jobID := core.JobID{1}
	coordinator := builderProcessCoordinator{result: runtimeengine.ProcessResult{
		Outcome: runtimeengine.ProcessClaimed, JobID: jobID, FailedStage: "generate_bootstrap",
	}, err: claimErr}
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(coordinator, "linux", 1),
		runnerPool: "linux", stateBackend: "sqlite", report: collectBuilderPoolEvents(t, &reported)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.RequestWork(ctx, "linux"); !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v, want cancellation", err)
	}
	if len(reported) != 2 || reported[0].operation != "claim" || reported[0].outcome != "claimed" ||
		reported[1].operation != "job_processing" || reported[1].outcome != "failed" ||
		!errors.Is(reported[1].err, context.Canceled) {
		t.Fatalf("claimed job events = %+v", reported)
	}
}

type builderProcessCoordinator struct {
	result runtimeengine.ProcessResult
	err    error
}

func (builderProcessCoordinator) OccupiedSlots(context.Context, core.RunnerPool) (int, error) {
	return 0, nil
}

func (coordinator builderProcessCoordinator) Process(context.Context, core.RunnerPool) (runtimeengine.ProcessResult, error) {
	return coordinator.result, coordinator.err
}

func newBuilderOrchestrator(
	t *testing.T,
	store *builderStoreFake,
	manager *githubsource.Manager,
	provider builderProviderFake,
) *runtimeengine.Orchestrator {
	t.Helper()
	resolver, err := newSourceOperationsResolver([]sourceOperationsRegistration{{
		adapter: githubsource.AdapterName, operations: manager.Source(),
	}})
	if err != nil {
		t.Fatalf("source operations resolver: %v", err)
	}
	return runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store, resolver, provider, provider, runtimeengine.OrchestratorConfig{MemberID: daemonTestID(1), CurrentTime: builderTime, StopIfNotStartedWithin: 5 * time.Minute},
	)
}

func assertPoolAdapterSuccess(t *testing.T, orchestrator *runtimeengine.Orchestrator) {
	t.Helper()
	pool := orchestrationPool{worker: runtimeengine.NewPoolWorker(orchestrator, "linux", 1), orchestrator: orchestrator}
	if _, err := pool.RequestWork(context.Background(), "linux"); err != nil {
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
	if _, err := pool.RequestWork(context.Background(), "linux"); !errors.Is(err, wantErr) {
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
			Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
		}},
	}
}

func TestUnifiedBuilderIdentifiesStateAndRejectsMissingProviderSelection(t *testing.T) {
	if got := stateBackendName(config.StateConfig{Postgres: &postgresconfig.Settings{}}); got != "postgresql" {
		t.Fatalf("state backend = %q", got)
	}
	invalid := config.RunnerPoolConfig{Name: "linux"}
	if _, err := configuredPoolProviderKind(invalid); err == nil {
		t.Fatal("missing provider selection was accepted")
	}
	assembled := assembledRuntime{}
	if err := assembled.appendRunnerPool(invalid, unifiedBuilderDependencies{}); err == nil {
		t.Fatal("invalid Runner Pool was appended")
	}
}

func TestConfiguredEnqueueWakeKeepsPostgreSQLMembersOnTheSharedWakePath(t *testing.T) {
	wakes := 0
	wake := func(core.RunnerPool) { wakes++ }

	if configuredEnqueueWake("postgresql", wake) != nil {
		t.Fatal("PostgreSQL configured a process-local enqueue wake")
	}
	sqliteWake := configuredEnqueueWake("sqlite", wake)
	if sqliteWake == nil {
		t.Fatal("SQLite did not configure its process-local enqueue wake")
	}
	sqliteWake("linux")
	if wakes != 1 {
		t.Fatalf("SQLite enqueue wakes = %d, want 1", wakes)
	}
}

func TestQueueNotificationReportingUsesAnIndependentOperationalStatus(t *testing.T) {
	wantCause := errors.New("listener failed")
	reported := make(chan error, 2)
	reportQueueNotificationStatus(func(err error) { reported <- err }, wantCause)

	var failure operationalEventEnvelope
	if err := <-reported; !errors.As(err, &failure) || !errors.Is(err, wantCause) {
		t.Fatalf("failure report = %v, want operational envelope containing %v", err, wantCause)
	}
	if failure.event.operation != "queue_notifications" || failure.event.outcome != "failed" ||
		failure.event.stateBackend != "postgresql" || failure.event.stateOperation != "listen" {
		t.Fatalf("failure event = %+v", failure.event)
	}
	if failure.event.err == nil || errors.Is(failure.event.err, wantCause) {
		t.Fatalf("operator error = %v, want sanitized notification failure", failure.event.err)
	}

	reportQueueNotificationStatus(func(err error) { reported <- err }, nil)
	var recovery operationalEventEnvelope
	if err := <-reported; !errors.As(err, &recovery) || recovery.event.outcome != "recovered" {
		t.Fatalf("recovery report = %v, want recovered operational envelope", err)
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
func (*builderStoreFake) ReconcileAssignedDemand(
	context.Context,
	githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	return githubsource.AssignedDemandResult{}, nil
}
func (store *builderStoreFake) ClaimOldest(_ context.Context, _ core.RunnerPool, _ core.ProcessingClaim) (core.ClaimResult, error) {
	return core.ClaimResult{Outcome: core.ClaimEmpty}, store.operationErr
}
func (*builderStoreFake) Terminalize(context.Context, core.JobIdentity) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (*builderStoreFake) ObserveRunnerAcceptance(context.Context, core.JobIdentity) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (*builderStoreFake) ObserveTerminalIfUnaccepted(context.Context, core.JobIdentity) (core.ConditionalTerminalResult, error) {
	return core.ConditionalTerminalResult{}, nil
}
func (store *builderStoreFake) ProcessingByMember(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
	return nil, store.operationErr
}
func (store *builderStoreFake) ProcessingByPool(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
	return nil, store.operationErr
}
func (*builderStoreFake) RetryUnstarted(context.Context, core.ProcessingJob) (core.RetryResult, error) {
	return core.RetryResult{Outcome: core.RetryMissing}, nil
}
func (*builderStoreFake) RecoverUnstartedProcessing(context.Context, core.ProcessingJob) (core.RetryResult, error) {
	return core.RetryResult{Outcome: core.RetryMissing}, nil
}
func (*builderStoreFake) RemoveTerminal(context.Context, core.JobID) (core.TerminalRemovalResult, error) {
	return core.TerminalRemovalResult{Outcome: core.TerminalMissing}, nil
}
func (*builderStoreFake) ReserveProvisioningFailure(context.Context, core.ProcessingJob) (core.RetryResult, error) {
	return core.RetryResult{Outcome: core.RetryFailureReportingPending}, nil
}
func (*builderStoreFake) RecordProvisioningFailureReported(context.Context, core.ProcessingJob) (core.RetryResult, error) {
	return core.RetryResult{Outcome: core.RetryFailureFinalizationPending}, nil
}
func (*builderStoreFake) FinalizeProvisioningFailure(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
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
func (store *builderStoreFake) OwnedRegistrations(context.Context) ([]githubsource.Registration, error) {
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
func (*builderStoreFake) DeleteActiveRegistration(context.Context, githubsource.Registration) error {
	return nil
}
func (*builderStoreFake) DeleteOwnedRegistration(context.Context, githubsource.Registration) error {
	return nil
}
func (*builderStoreFake) DeleteRegistrationState(context.Context, githubsource.Registration) error {
	return nil
}
func (*builderStoreFake) DeleteRepositoryRegistrationState(
	context.Context,
	string,
	transport.InstallationID,
) error {
	return nil
}
