//go:build e2e

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

const e2eWaitLimit = 10 * time.Second

type e2eHarness struct {
	t                         *testing.T
	cancel                    context.CancelFunc
	configured                config.Config
	runtime                   *productionRuntime
	generation                *runtimeGeneration
	maintenanceGeneration     *maintenanceGeneration
	host                      *e2eGitHost
	provider                  *e2eProvider
	windowsProvider           *e2eProvider
	providerBoundary          execution.RunnerLauncher
	windowsProviderBoundary   execution.RunnerLauncher
	providerInventory         execution.RunnerInventory
	windowsProviderInventory  execution.RunnerInventory
	sourceRuntime             *githubSourceRuntime
	sourceActivities          *e2eSupervisorActivityRecorder
	registrationTargets       map[string]uint64
	sessionTargets            map[string]uint64
	pollTargets               map[string]uint64
	membership                *e2eManualTicker
	source                    *e2eManualTicker
	execution                 *e2eManualTicker
	currentTime               time.Time
	clockTicks                atomic.Int64
	clockAdvance              atomic.Int64
	memberName                string
	disableQueueNotifications bool
	notificationReady         chan struct{}
	notificationReadyOnce     sync.Once
	laneCompleted             map[*e2eManualTicker]*e2eLaneBarrier
}

type githubSessionHandler interface {
	core.SourceEventHandler
	githubsource.AssignedDemandHandler
}

type githubSourceRuntime struct {
	capacityProvider core.CapacitySnapshotProvider
}

func (runtime *githubSourceRuntime) AvailableRunnerPoolCapacity(
	ctx context.Context,
	runnerPool core.RunnerPool,
	_ time.Time,
) (int, error) {
	snapshot, err := runtime.capacityProvider.CapacitySnapshot(ctx, runnerPool)
	if err != nil {
		return 0, fmt.Errorf("read available Runner Pool capacity: %w", err)
	}
	return snapshot.Available, nil
}

type e2eSupervisorActivityRecorder struct {
	mutex        sync.Mutex
	activities   []e2eRecordedSupervisorActivity
	nextSequence uint64
	changed      chan struct{}
}

type e2eRecordedSupervisorActivity struct {
	sequence uint64
	activity githubsource.ManagerActivity
}

func newE2ESupervisorActivityRecorder() *e2eSupervisorActivityRecorder {
	return &e2eSupervisorActivityRecorder{changed: make(chan struct{})}
}

func (recorder *e2eSupervisorActivityRecorder) observe(
	observer githubsource.ManagerObserver,
) githubsource.ManagerObserver {
	report := observer.Report
	observer.Report = func(activity githubsource.ManagerActivity) {
		if report != nil {
			report(activity)
		}
		recorder.record(activity)
	}
	return observer
}

func (recorder *e2eSupervisorActivityRecorder) record(activity githubsource.ManagerActivity) {
	recorder.mutex.Lock()
	recorder.nextSequence++
	recorder.activities = append(recorder.activities, e2eRecordedSupervisorActivity{
		sequence: recorder.nextSequence,
		activity: activity,
	})
	close(recorder.changed)
	recorder.changed = make(chan struct{})
	recorder.mutex.Unlock()
}

func (recorder *e2eSupervisorActivityRecorder) checkpoint() uint64 {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.nextSequence
}

func (recorder *e2eSupervisorActivityRecorder) awaitAfter(
	t *testing.T,
	description string,
	checkpoint uint64,
	matches func(githubsource.ManagerActivity) bool,
) githubsource.ManagerActivity {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		recorder.mutex.Lock()
		for _, recorded := range recorder.activities {
			if recorded.sequence > checkpoint && matches(recorded.activity) {
				recorder.mutex.Unlock()
				return recorded.activity
			}
		}
		changed := recorder.changed
		recorder.mutex.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
			return githubsource.ManagerActivity{}
		}
	}
}

func newSQLiteE2EHarness(t *testing.T) *e2eHarness {
	t.Helper()
	return newE2EHarness(t, config.StateConfig{Path: ":memory:"}, "e2e-member", false)
}

func newPostgresE2EHarness(t *testing.T) *e2eHarness {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	return newE2EHarness(t, config.StateConfig{Postgres: &settings}, "e2e-member", false)
}

func newE2EHarness(
	t *testing.T,
	state config.StateConfig,
	memberName string,
	disableQueueNotifications bool,
) *e2eHarness {
	t.Helper()
	return newE2EHarnessWithProviderEndpoints(
		t,
		state,
		memberName,
		disableQueueNotifications,
		execution.InventoryEndpointID("e2e:linux:"+memberName),
		execution.InventoryEndpointID("e2e:windows:"+memberName),
	)
}

func newE2EHarnessWithProviderEndpoints(
	t *testing.T,
	state config.StateConfig,
	memberName string,
	disableQueueNotifications bool,
	linuxEndpoint execution.InventoryEndpointID,
	windowsEndpoint execution.InventoryEndpointID,
) *e2eHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	harness := newE2EHarnessFixture(t, cancel, memberName, disableQueueNotifications, linuxEndpoint, windowsEndpoint)
	harness.initializeObservationTargets()
	harness.configured = e2eConfiguration(state)
	harness.openAndActivate(ctx)
	t.Cleanup(harness.close)
	return harness
}

func newE2EHarnessFixture(
	t *testing.T,
	cancel context.CancelFunc,
	memberName string,
	disableQueueNotifications bool,
	linuxEndpoint execution.InventoryEndpointID,
	windowsEndpoint execution.InventoryEndpointID,
) *e2eHarness {
	t.Helper()
	return &e2eHarness{
		t: t, cancel: cancel, host: newE2EGitHost(),
		provider:        newE2EProviderForEndpoint(linuxEndpoint),
		windowsProvider: newE2EProviderForEndpoint(windowsEndpoint),
		membership:      newE2EManualTicker(), source: newE2EManualTicker(), execution: newE2EManualTicker(),
		currentTime: time.Date(2020, time.August, 16, 12, 0, 0, 0, time.UTC), memberName: memberName,
		disableQueueNotifications: disableQueueNotifications,
		notificationReady:         make(chan struct{}),
		laneCompleted:             make(map[*e2eManualTicker]*e2eLaneBarrier),
		sourceRuntime:             &githubSourceRuntime{},
		sourceActivities:          newE2ESupervisorActivityRecorder(),
	}
}

func (harness *e2eHarness) initializeObservationTargets() {
	runnerPools := []string{"linux", "windows"}
	harness.registrationTargets = e2eRouteTargets(runnerPools, harness.host.registrationObservation)
	harness.sessionTargets = e2eRouteTargets(runnerPools, harness.host.sessionObservation)
	harness.pollTargets = e2eSessionPollTargets(runnerPools, harness.host)
	for _, ticker := range []*e2eManualTicker{harness.membership, harness.source, harness.execution} {
		harness.laneCompleted[ticker] = newE2ELaneBarrier()
	}
}

func (harness *e2eHarness) openAndActivate(ctx context.Context) {
	harness.t.Helper()
	var err error
	harness.runtime, err = openProductionRuntimeWithDependencies(ctx, harness.configured, harness.dependencies())
	if err != nil {
		harness.cancel()
		harness.t.Fatalf("open production runtime: %v", err)
	}
	harness.generation, err = harness.runtime.Build(ctx, harness.configured)
	if err != nil {
		harness.shutdown(ctx)
		harness.t.Fatalf("build production generation: %v", err)
	}
	if err = harness.generation.Prepare(ctx); err != nil {
		harness.shutdown(ctx)
		harness.t.Fatalf("prepare production generation: %v", err)
	}
	if err = harness.generation.Activate(ctx); err != nil {
		harness.shutdown(ctx)
		harness.t.Fatalf("activate production generation: %v", err)
	}
	for _, ticker := range []*e2eManualTicker{harness.membership, harness.source, harness.execution} {
		harness.laneCompleted[ticker].waitAfter(harness.t, "initial production maintenance lane", 0)
	}
}

func (harness *e2eHarness) dependencies() productionDependencies {
	dependencies := newProductionDependencies()
	dependencies.logDestinationFactory = e2eLogFactory{}
	dependencies.readFile = func(string) ([]byte, error) { return []byte("e2e-private-key"), nil }
	dependencies.readHostname = harness.readHostname
	dependencies.currentTime = harness.now
	dependencies.newDiscoverer = newE2EDiscoverer
	dependencies.newQueueNotificationPool = harness.newQueueNotificationPool
	dependencies.newTicker = func(time.Duration) cycleTicker { return harness.nextTicker() }
	dependencies.maintenanceLaneCompleted = harness.observeMaintenanceLaneCompletion
	dependencies.newManager = harness.newManager
	dependencies.buildProviderSet = harness.buildProviderSet
	harness.captureMaintenanceGeneration(&dependencies)
	return dependencies
}

func (harness *e2eHarness) captureMaintenanceGeneration(dependencies *productionDependencies) {
	buildGeneration := dependencies.buildGeneration
	dependencies.buildGeneration = func(
		ctx context.Context,
		configured config.Config,
		state *productionState,
		gate *localLeadershipGate,
		report func(error),
		production productionDependencies,
	) (*maintenanceGeneration, error) {
		if harness.sourceRuntime.capacityProvider == nil {
			harness.sourceRuntime.capacityProvider = runtimeengine.NewCapacitySnapshotService(
				state.store.capacity, production.currentTime, runtimeMemberTTL,
			)
			state.store.capacity = harness.sourceRuntime
		}
		generation, err := buildGeneration(ctx, configured, state, gate, report, production)
		if err != nil {
			return nil, err
		}
		harness.maintenanceGeneration = generation
		return generation, nil
	}
}

func (harness *e2eHarness) markLinuxProcessorIneligible() {
	harness.t.Helper()
	if harness.maintenanceGeneration == nil || harness.maintenanceGeneration.processor == nil {
		harness.t.Fatal("production queue processor was not captured")
	}
	pool := harness.maintenanceGeneration.processor.gate.pool("linux")
	pool.mutex.Lock()
	pool.eligible = false
	pool.mutex.Unlock()
}

func (harness *e2eHarness) observeMaintenanceLaneCompletion(_ string, observed cycleTicker) {
	ticker, validTicker := observed.(*e2eManualTicker)
	if !validTicker {
		harness.t.Errorf("completed production maintenance ticker = %T, want *e2eManualTicker", observed)
		return
	}
	barrier := harness.laneCompleted[ticker]
	if barrier == nil {
		harness.t.Errorf("completed an unknown production maintenance ticker")
		return
	}
	barrier.complete()
}

func (harness *e2eHarness) readHostname() (string, error) { return harness.memberName, nil }

func (harness *e2eHarness) now() time.Time {
	return harness.currentTime.Add(
		time.Duration(harness.clockTicks.Add(1))*time.Millisecond +
			time.Duration(harness.clockAdvance.Load()),
	)
}

func (harness *e2eHarness) advanceClock(duration time.Duration) {
	harness.clockAdvance.Add(int64(duration))
}

func newE2EDiscoverer(transport.Credentials, http.RoundTripper) (transport.Discoverer, error) {
	return transport.Discoverer{}, nil
}

func (harness *e2eHarness) nextTicker() *e2eManualTicker {
	tickers := []*e2eManualTicker{harness.membership, harness.source, harness.execution}
	for _, ticker := range tickers {
		if !ticker.assigned.Swap(true) {
			return ticker
		}
	}
	harness.t.Fatal("production requested an unexpected maintenance ticker")
	return nil
}

func (harness *e2eHarness) newManager(
	_ githubsource.RepositoryDiscoverer,
	_ githubsource.ManagerClientFactoryFunc,
	registrations githubsource.RegistrationRepository,
	observer githubsource.ManagerObserver,
) (*githubsource.Manager, error) {
	observer = e2eObserveHostManagerActivities(harness.host, observer)
	observer = harness.sourceActivities.observe(observer)
	manager, err := githubsource.NewManager(harness.host, harness.host.Client, registrations, observer)
	if err != nil {
		return nil, fmt.Errorf("build scripted GitHub manager: %w", err)
	}
	return manager, nil
}

func (harness *e2eHarness) buildProviderSet(
	ctx context.Context,
	_ config.Config,
	_ core.MemberID,
	_ productionDependencies,
) (map[core.RunnerPool]execution.RunnerLauncher, runtimeengine.AggregateRunnerInventory, error) {
	harness.providerBoundary = runtimeengine.WithLauncherOperationDeadlines(
		harness.provider, "e2e",
	)
	harness.windowsProviderBoundary = runtimeengine.WithLauncherOperationDeadlines(
		harness.windowsProvider, "e2e-windows",
	)
	harness.providerInventory = runtimeengine.WithInventoryOperationDeadlines(harness.provider, "e2e")
	harness.windowsProviderInventory = runtimeengine.WithInventoryOperationDeadlines(harness.windowsProvider, "e2e-windows")
	if err := harness.providerBoundary.Validate(ctx); err != nil {
		return nil, runtimeengine.AggregateRunnerInventory{}, fmt.Errorf("validate scripted Linux provider: %w", err)
	}
	if err := harness.windowsProviderBoundary.Validate(ctx); err != nil {
		return nil, runtimeengine.AggregateRunnerInventory{}, fmt.Errorf("validate scripted Windows provider: %w", err)
	}
	inventory := runtimeengine.NewAggregateRunnerInventory(harness.providerInventory, harness.windowsProviderInventory)
	return map[core.RunnerPool]execution.RunnerLauncher{
		"linux": harness.providerBoundary, "windows": harness.windowsProviderBoundary,
	}, inventory, nil
}

func (harness *e2eHarness) newQueueNotificationPool(pool *pgxpool.Pool) queueNotificationPool {
	if harness.disableQueueNotifications {
		return queueNotificationPool{maxConnections: 1}
	}
	configured := newPGXQueueNotificationPool(pool)
	acquire := configured.acquire
	configured.acquire = func(ctx context.Context) (queueNotificationConnection, error) {
		connection, err := acquire(ctx)
		if err != nil {
			return queueNotificationConnection{}, err
		}
		listen := connection.listen
		connection.listen = func(listenContext context.Context) error {
			if listenErr := listen(listenContext); listenErr != nil {
				return listenErr
			}
			harness.notificationReadyOnce.Do(func() { close(harness.notificationReady) })
			return nil
		}
		return connection, nil
	}
	return configured
}

func e2eConfiguration(state config.StateConfig) config.Config {
	return config.Config{
		State: state,
		GitHub: config.GitHubConfig{
			Apps: []config.GitHubAppConfig{{AppID: 1, PrivateKeyFile: "e2e-private-key.pem"}}, DiscoveryInterval: "1ms",
		},
		RunnerPools: []config.RunnerPoolConfig{
			{
				Name: "linux", RunnerGroup: "default", MaxRunners: 1,
				Docker: &config.DockerConfig{ComposeFile: "unused-e2e-compose.yaml", Service: "runner"},
			},
			{
				Name: "windows", RunnerGroup: "default", MaxRunners: 1,
				Docker: &config.DockerConfig{ComposeFile: "unused-e2e-compose.yaml", Service: "runner"},
			},
		},
	}
}

func e2eAppRegistrations(
	t *testing.T,
	store githubsource.RegistrationRepository,
) *githubsource.AppScopedRegistrationRepository {
	t.Helper()
	scoped, err := githubsource.ScopeRegistrationRepository(store, 1)
	if err != nil {
		t.Fatalf("scope E2E GitHub registrations: %v", err)
	}
	return scoped
}

func e2eBackendEnabled(backend string) bool {
	return selectedE2EBackend.enables(backend)
}

func (harness *e2eHarness) establishSourceSession() {
	harness.t.Helper()
	harness.await("initial provider inventory", harness.provider.refreshed)
	harness.completeMaintenanceCycle(harness.membership)
	harness.reconcileSourceAfterMembership(harness.registrationTargets)
	e2eAwaitRouteTargetsWhileCycling(harness.t, "GitHub message sessions", harness.sessionTargets,
		harness.host.sessionObservation, func() { harness.completeMaintenanceCycle(harness.source) })
	e2eAwaitSessionPollTargetsWhileCycling(harness.t, "GitHub message polls", harness.pollTargets,
		harness.host, func() { harness.completeMaintenanceCycle(harness.source) })
	if harness.configured.State.Postgres != nil && !harness.disableQueueNotifications {
		harness.await("PostgreSQL queue listener readiness", harness.notificationReady)
	}
}

func (harness *e2eHarness) reconcileSourceAfterMembership(registrationTargets map[string]uint64) {
	harness.t.Helper()
	discoveryInterval, err := harness.configured.GitHub.DiscoveryDuration()
	if err != nil {
		harness.t.Fatalf("read E2E GitHub discovery interval: %v", err)
	}
	e2eAwaitRouteTargetsWhileCycling(harness.t, "post-membership source reconciliation",
		registrationTargets, harness.host.registrationObservation,
		func() { harness.runSourceAtNextDiscovery(discoveryInterval) })
}

func e2eRouteTargets(
	runnerPools []string,
	observation func(string) (uint64, <-chan struct{}),
) map[string]uint64 {
	targets := make(map[string]uint64, len(runnerPools))
	for _, runnerPool := range runnerPools {
		sequence, _ := observation(runnerPool)
		targets[runnerPool] = sequence + 1
	}
	return targets
}

func e2eSessionPollTargets(runnerPools []string, host *e2eGitHost) map[string]uint64 {
	targets := make(map[string]uint64, len(runnerPools))
	for _, runnerPool := range runnerPools {
		sequence, _ := host.sessions[runnerPool].pollObservation()
		targets[runnerPool] = sequence + 1
	}
	return targets
}

func e2eAwaitRouteTargetsWhileCycling(
	t *testing.T,
	description string,
	targets map[string]uint64,
	observation func(string) (uint64, <-chan struct{}),
	cycle func(),
) {
	t.Helper()
	e2eAwaitTargetsWhileCycling(t, description, targets, func(runnerPool string) uint64 {
		sequence, _ := observation(runnerPool)
		return sequence
	}, cycle)
}

func e2eAwaitSessionPollTargetsWhileCycling(
	t *testing.T,
	description string,
	targets map[string]uint64,
	host *e2eGitHost,
	cycle func(),
) {
	t.Helper()
	e2eAwaitTargetsWhileCycling(t, description, targets, func(runnerPool string) uint64 {
		sequence, _ := host.sessions[runnerPool].pollObservation()
		return sequence
	}, cycle)
}

func e2eAwaitTargetsWhileCycling(
	t *testing.T,
	description string,
	targets map[string]uint64,
	sequence func(string) uint64,
	cycle func(),
) {
	t.Helper()
	completed := make(map[string]bool, len(targets))
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		pending := e2ePendingTargets(targets, completed, sequence)
		if len(pending) == 0 {
			return
		}
		cycle()
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for %s; pending Runner Pools %v", description, pending)
		default:
		}
	}
}

func e2ePendingTargets(
	targets map[string]uint64,
	completed map[string]bool,
	sequence func(string) uint64,
) []string {
	pending := make([]string, 0, len(targets))
	for runnerPool, target := range targets {
		if completed[runnerPool] {
			continue
		}
		if sequence(runnerPool) >= target {
			completed[runnerPool] = true
			continue
		}
		pending = append(pending, runnerPool)
	}
	return pending
}

func (harness *e2eHarness) runSourceAtNextDiscovery(discoveryInterval time.Duration) {
	harness.t.Helper()
	harness.advanceClock(discoveryInterval)
	harness.completeMaintenanceCycle(harness.membership)
	harness.completeMaintenanceCycle(harness.source)
}

func (harness *e2eHarness) completeMaintenanceCycle(ticker *e2eManualTicker) {
	harness.t.Helper()
	barrier := harness.laneCompleted[ticker]
	before := barrier.count()
	if !ticker.Tick(harness.currentTime) {
		harness.t.Fatal("cannot run production maintenance: ticker is stopped")
	}
	barrier.waitAfter(harness.t, "production maintenance lane completion", before)
}

type e2eLaneBarrier struct {
	mutex     sync.Mutex
	completed uint64
	changed   chan struct{}
}

func newE2ELaneBarrier() *e2eLaneBarrier {
	return &e2eLaneBarrier{changed: make(chan struct{})}
}

func (barrier *e2eLaneBarrier) count() uint64 {
	barrier.mutex.Lock()
	defer barrier.mutex.Unlock()
	return barrier.completed
}

func (barrier *e2eLaneBarrier) complete() {
	barrier.mutex.Lock()
	barrier.completed++
	close(barrier.changed)
	barrier.changed = make(chan struct{})
	barrier.mutex.Unlock()
}

func (barrier *e2eLaneBarrier) waitAfter(t *testing.T, description string, before uint64) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		barrier.mutex.Lock()
		if barrier.completed > before {
			barrier.mutex.Unlock()
			return
		}
		changed := barrier.changed
		barrier.mutex.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func (harness *e2eHarness) deliver(message *upstream.RunnerScaleSetMessage) {
	harness.deliverTo("linux", message)
}

func (harness *e2eHarness) deliverTo(runnerPool string, message *upstream.RunnerScaleSetMessage) {
	harness.t.Helper()
	session := harness.host.sessions[runnerPool]
	messageID := session.assignMessageID(message)
	session.messages <- message
	harness.awaitMessageAcknowledgement(session, messageID)
}

func (harness *e2eHarness) awaitMessageAcknowledgement(session *e2eGitHubSession, messageID int) {
	harness.t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case <-session.acknowledgement(messageID):
	case <-timer.C:
		harness.t.Fatalf("timed out waiting for GitHub message %d acknowledgement", messageID)
	}
	harness.await("GitHub message poll completion", session.pollCompletion(messageID))
	pollSequence, _ := session.pollObservation()
	e2eAwaitSessionPollWhileCycling(harness.t, "GitHub message poll harvest", session,
		pollSequence+1, func() { harness.completeMaintenanceCycle(harness.source) })
}

func e2eAwaitSessionPollWhileCycling(
	t *testing.T,
	description string,
	session interface {
		pollObservation() (uint64, <-chan struct{})
	},
	target uint64,
	cycle func(),
) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		sequence, _ := session.pollObservation()
		if sequence >= target {
			return
		}
		cycle()
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		default:
		}
	}
}

func (harness *e2eHarness) await(description string, signal <-chan struct{}) {
	harness.t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		harness.t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitProviderStart(t *testing.T, description string, values <-chan execution.StartRequest) execution.StartRequest {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		return execution.StartRequest{}
	}
}

func awaitRunnerInstance(
	t *testing.T,
	description string,
	values <-chan execution.RunnerInstance,
) execution.RunnerInstance {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		return execution.RunnerInstance{}
	}
}

func awaitString(t *testing.T, description string, values <-chan string) string {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		return ""
	}
}

func (harness *e2eHarness) close() {
	shutdownContext, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	harness.shutdown(shutdownContext)
}

func (harness *e2eHarness) shutdown(ctx context.Context) {
	if harness.generation != nil {
		if err := harness.generation.Close(ctx); err != nil {
			harness.t.Errorf("close production generation: %v", err)
		}
		harness.generation = nil
	}
	if harness.runtime != nil {
		if err := harness.runtime.Close(ctx); err != nil {
			harness.t.Errorf("close production runtime: %v", err)
		}
		harness.runtime = nil
	}
	harness.cancel()
}

type e2eLogFactory struct{}

func (e2eLogFactory) Open(config.LogConfig) (io.WriteCloser, error) {
	return e2eBufferCloser{Buffer: &bytes.Buffer{}}, nil
}

type e2eBufferCloser struct{ *bytes.Buffer }

func (e2eBufferCloser) Close() error { return nil }

func drainSignals(signals <-chan struct{}) {
	for {
		select {
		case <-signals:
		default:
			return
		}
	}
}

func e2eMessageStatistics(assigned int) *upstream.RunnerScaleSetStatistic {
	return &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: assigned}
}

func describeProcessing(records []core.ProcessingJob) string {
	return fmt.Sprintf("%+v", records)
}

func assertExactDemandState(
	t *testing.T,
	harness *e2eHarness,
	requestID int64,
	want core.QueueOutcome,
) core.JobID {
	t.Helper()
	const runnerPool core.RunnerPool = "linux"
	identity := githubsource.SourceIdentity{
		Repository: harness.host.repository.URL, RunnerPool: runnerPool, RequestID: requestID,
	}
	sourceID, err := githubsource.EncodeSourceID(identity)
	if err != nil {
		t.Fatalf("encode GitHub source identity: %v", err)
	}
	scope, err := githubsource.DemandScope(identity)
	if err != nil {
		t.Fatalf("encode GitHub demand scope: %v", err)
	}
	result, err := harness.runtime.state.store.jobs.Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}, Scope: scope,
		RunnerPool: runnerPool, EnqueuedAt: harness.currentTime,
	})
	if err != nil || result.Outcome != want {
		t.Fatalf("exact demand state = %+v, error = %v, want %s", result, err, want)
	}
	return result.Job.ID
}
