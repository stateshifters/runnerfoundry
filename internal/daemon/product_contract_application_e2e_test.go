//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/state/sqlite"
	"gopkg.in/yaml.v3"
)

func TestProductContractApplicationConfigurationReloadHealthAndShutdown(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	application := newE2EApplication(t)
	application.provider.setInspectionError(errors.New("scripted initial inventory failure"))
	running := startE2EApplication(t, application)

	application.awaitReady(t, "initial application readiness")
	awaitConfiguredBuild(t, application.builtConfigs)
	application.tickers.awaitInitialGeneration(t)
	application.awaitHealth(t, healthDegraded)
	assertApplicationStartupOperatorSnapshot(t, application)
	application.provider.setInspectionError(nil)
	application.tickers.cycle(t, "membership")
	application.tickers.cycle(t, "source")
	application.tickers.cycle(t, "execution")
	application.awaitHealth(t, healthOperational)
	assertApplicationRecoveryOperatorSnapshot(t, application)

	application.rejectInvalidReload(t)
	application.applyValidReload(t)
	application.tickers.awaitInitialGeneration(t)
	assertApplicationReloadOperatorSnapshot(t, application)
	notificationCount := len(application.notifier.snapshot())
	logCount := len(application.logs.records(t))
	running.cancel()
	if err := awaitApplicationResult(t, running.result); err != nil {
		t.Fatalf("run production daemon application: %v", err)
	}
	assertApplicationShutdownOperatorSnapshot(t, application, notificationCount, logCount)
	assertApplicationCapacityWithdrawn(t, application.databasePath)
}

func TestProductContractApplicationShutdownPolicies(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	for _, policy := range []ActiveRunnerPolicy{LeaveActiveRunners, TerminateActiveRunners} {
		t.Run(string(policy), func(t *testing.T) {
			application := newE2EApplication(t)
			application.enableSource(policy)
			running := startE2EApplication(t, application)
			application.awaitReady(t, "application readiness for "+string(policy))
			awaitConfiguredBuild(t, application.builtConfigs)
			application.tickers.awaitInitialGeneration(t)
			if observed := awaitApplicationPolicy(t, application.observedPolicy); observed != policy {
				t.Fatalf("supervisor policy = %q, want %q", observed, policy)
			}
			application.establishSource(t)
			linux, windows := application.startShutdownPolicyJobs(t)
			running.cancel()
			if err := awaitApplicationResult(t, running.result); err != nil {
				t.Fatalf("run %s shutdown: %v", policy, err)
			}
			application.assertShutdownPolicyState(t, policy, linux, windows)
		})
	}
}

type e2eApplicationRun struct {
	cancel   context.CancelFunc
	result   chan error
	finished chan struct{}
}

func startE2EApplication(t *testing.T, application *e2eApplication) e2eApplicationRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	running := e2eApplicationRun{cancel: cancel, result: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		defer close(running.finished)
		running.result <- runDaemon(ctx, application.options(), application.dependencies())
	}()
	t.Cleanup(func() {
		cancel()
		timer := time.NewTimer(e2eWaitLimit)
		defer timer.Stop()
		select {
		case <-running.finished:
		case <-timer.C:
			t.Error("production application did not stop during test cleanup")
		}
	})
	return running
}

type e2eApplication struct {
	databasePath                     string
	memberName                       string
	host                             *e2eGitHost
	replacementHost                  *e2eGitHost
	provider                         *e2eProvider
	replacementProvider              *e2eProvider
	windows                          *e2eProvider
	replacementWindows               *e2eProvider
	notifier                         *e2eApplicationNotifier
	logs                             *e2eApplicationLogDestination
	tickers                          *e2eApplicationTickers
	builtConfigs                     chan config.Config
	builtSources                     chan e2eGitHubSourceRuntimeResult
	reloads                          chan struct{}
	observedPolicy                   chan ActiveRunnerPolicy
	mutex                            sync.Mutex
	configured                       config.Config
	runtime                          *productionRuntime
	policy                           ActiveRunnerPolicy
	clockAdvance                     atomic.Int64
	shutdownTimerFactory             func(time.Duration) shutdownTimer
	runtimeClosed                    chan struct{}
	runtimeCloseOnce                 sync.Once
	managerBuilds                    int
	providerBuilds                   int
	generationDecorator              func(int, generation) generation
	replacementProviderValidationErr error
	useProductionProviders           bool
	dockerCommands                   dockerprovider.CommandRunner
}

func (application *e2eApplication) enableSource(policy ActiveRunnerPolicy) {
	application.host.withDefaultRepository()
	application.policy = policy
}

func newE2EApplication(t *testing.T) *e2eApplication {
	t.Helper()
	return newE2EApplicationWithState(
		t,
		config.StateConfig{Path: t.TempDir() + "/application.db"},
		"e2e-application-member",
	)
}

func newE2EApplicationWithState(
	t *testing.T,
	state config.StateConfig,
	memberName string,
) *e2eApplication {
	t.Helper()
	configured := e2eConfiguration(state)
	configured.Log.Format = "json"
	configured.Log.Level = "debug"
	configured.GitHub.Apps[0].PrivateKeyFile = "e2e-private-key.pem"
	configured.GitHub.DiscoveryInterval = "1m"
	application := &e2eApplication{
		databasePath: configured.State.Path, memberName: memberName, host: newE2EGitHost(),
		provider: newE2EProviderForEndpoint(execution.InventoryEndpointID("e2e:" + memberName + ":linux")),
		windows:  newE2EProviderForEndpoint(execution.InventoryEndpointID("e2e:" + memberName + ":windows")),
		notifier: newE2EApplicationNotifier(), logs: newE2EApplicationLogDestination(),
		tickers:      newE2EApplicationTickers(),
		builtConfigs: make(chan config.Config, 4), builtSources: make(chan e2eGitHubSourceRuntimeResult, 4),
		reloads: make(chan struct{}, 4), configured: configured,
		observedPolicy: make(chan ActiveRunnerPolicy, 1),
		runtimeClosed:  make(chan struct{}),
	}
	application.host.withoutRepositories()
	return application
}

func (application *e2eApplication) options() Options {
	return Options{ConfigPath: "e2e-config.yaml", ReloadRequests: application.reloads, ActiveRunnerPolicy: application.policy}
}

func (application *e2eApplication) dependencies() daemonApplicationDependencies {
	production := application.productionDependencies()
	dependencies := productionDaemonApplicationDependencies()
	dependencies.production = production
	dependencies.openRuntime = application.openRuntime
	dependencies.supervise = application.supervise
	if application.shutdownTimerFactory != nil {
		dependencies.newShutdownTimer = application.shutdownTimerFactory
	}
	return dependencies
}

func (application *e2eApplication) productionDependencies() productionDependencies {
	production := newProductionDependencies()
	production.readFile = application.readFile
	production.readHostname = func() (string, error) { return application.memberName, nil }
	production.logDestinationFactory = application.logs
	production.currentTime = application.now
	production.newDiscoverer = newE2EDiscoverer
	production.newTicker = func(duration time.Duration) cycleTicker {
		return application.tickers.nextTicker(duration)
	}
	production.maintenanceLaneCompleted = application.tickers.observeMaintenanceLaneCompletion
	production.newManager = application.newManager
	if application.useProductionProviders {
		production.dockerCommands = application.dockerCommands
	} else {
		production.buildProviderSet = application.buildProviderSet
	}
	application.observeSQLiteClose(&production)
	application.observeGenerationBuild(&production)
	return production
}

func (application *e2eApplication) observeSQLiteClose(production *productionDependencies) {
	closeSQLite := production.closeSQLite
	production.closeSQLite = func(store *sqlite.Store) error {
		err := closeSQLite(store)
		if err == nil {
			application.runtimeCloseOnce.Do(func() { close(application.runtimeClosed) })
		}
		return err
	}
}

func (application *e2eApplication) observeGenerationBuild(production *productionDependencies) {
	buildGeneration := production.buildGeneration
	production.buildGeneration = func(
		ctx context.Context,
		configured config.Config,
		state *productionState,
		gate *localLeadershipGate,
		report func(error),
		dependencies productionDependencies,
	) (*maintenanceGeneration, error) {
		application.builtConfigs <- configured
		generation, err := buildGeneration(ctx, configured, state, gate, report, dependencies)
		if err != nil {
			return nil, err
		}
		application.builtSources <- e2eGitHubSourceRuntime(generation)
		return generation, nil
	}
}

func (application *e2eApplication) now() time.Time {
	base := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(application.clockAdvance.Load()))
}

func (application *e2eApplication) advanceClock(duration time.Duration) {
	application.clockAdvance.Add(int64(duration))
}

func (application *e2eApplication) readFile(path string) ([]byte, error) {
	if path == "e2e-private-key.pem" {
		return []byte("e2e-private-key"), nil
	}
	if path != "e2e-config.yaml" {
		return nil, fmt.Errorf("unexpected E2E application file %q", path)
	}
	application.mutex.Lock()
	configured := application.configured
	application.mutex.Unlock()
	contents, err := yaml.Marshal(configured)
	if err != nil {
		return nil, fmt.Errorf("encode E2E application config: %w", err)
	}
	return contents, nil
}

func (application *e2eApplication) openRuntime(
	ctx context.Context,
	configured config.Config,
	dependencies productionDependencies,
) (*productionRuntime, error) {
	runtime, err := openProductionRuntimeWithDependencies(ctx, configured, dependencies)
	if err != nil {
		return nil, err
	}
	application.mutex.Lock()
	application.runtime = runtime
	application.mutex.Unlock()
	return runtime, nil
}

func (application *e2eApplication) supervise(
	ctx context.Context,
	reloads <-chan struct{},
	load func() (config.Config, error),
	build generationBuilder,
	_ serviceNotifier,
	report func(error),
	policy ActiveRunnerPolicy,
) error {
	application.observedPolicy <- policy
	application.mutex.Lock()
	application.runtime.reporter.notifier = application.notifier
	decorator := application.generationDecorator
	application.mutex.Unlock()
	buildNumber := 0
	decoratedBuild := func(buildContext context.Context, configured config.Config) (generation, error) {
		built, err := build(buildContext, configured)
		if err != nil {
			return nil, err
		}
		if decorator != nil {
			built = decorator(buildNumber, built)
		}
		buildNumber++
		return built, nil
	}
	return supervise(ctx, reloads, load, decoratedBuild, application.notifier, report, policy)
}

func (application *e2eApplication) newManager(
	_ githubsource.RepositoryDiscoverer,
	_ githubsource.ManagerClientFactoryFunc,
	registrations githubsource.RegistrationRepository,
	observer githubsource.ManagerObserver,
) (*githubsource.Manager, error) {
	application.mutex.Lock()
	host := application.host
	if application.managerBuilds > 0 && application.replacementHost != nil {
		host = application.replacementHost
	}
	application.managerBuilds++
	application.mutex.Unlock()
	observer = e2eObserveHostManagerActivities(host, observer)
	manager, err := githubsource.NewManager(host, host.Client, registrations, observer)
	if err != nil {
		return nil, fmt.Errorf("create E2E GitHub manager: %w", err)
	}
	return manager, nil
}

func (application *e2eApplication) rejectInvalidReload(t *testing.T) {
	t.Helper()
	application.mutex.Lock()
	application.configured.RunnerPools[0].MaxRunners = 0
	application.mutex.Unlock()
	application.reloads <- struct{}{}
	application.awaitReloading(t, "invalid reload notification")
	application.awaitReady(t, "readiness restoration after invalid reload")
}

func (application *e2eApplication) applyValidReload(t *testing.T) {
	t.Helper()
	application.mutex.Lock()
	application.configured.RunnerPools[0].MaxRunners = 2
	application.mutex.Unlock()
	application.reloads <- struct{}{}
	application.awaitReloading(t, "valid reload notification")
	built := awaitConfiguredBuild(t, application.builtConfigs)
	if built.RunnerPools[0].MaxRunners != 2 {
		t.Fatalf("replacement build capacity = %d, want 2", built.RunnerPools[0].MaxRunners)
	}
	application.awaitReady(t, "replacement readiness")
}

func (application *e2eApplication) awaitReady(t *testing.T, description string) {
	t.Helper()
	awaitSignal(t, description, application.notifier.ready)
}

func (application *e2eApplication) awaitReloading(t *testing.T, description string) {
	t.Helper()
	awaitSignal(t, description, application.notifier.reloading)
}

func (application *e2eApplication) awaitHealth(t *testing.T, want operationalHealth) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		select {
		case status := <-application.notifier.statuses:
			if status.health == want {
				return
			}
		case <-timer.C:
			application.mutex.Lock()
			runtime := application.runtime
			application.mutex.Unlock()
			runtime.reporter.mutex.Lock()
			failures := fmt.Sprintf("%+v", runtime.reporter.failures)
			lastStatus := runtime.reporter.lastStatus
			runtime.reporter.mutex.Unlock()
			t.Fatalf("timed out waiting for application health %q; failures=%s last=%+v", want, failures, lastStatus)
		}
	}
}

func (application *e2eApplication) establishSource(t *testing.T) {
	t.Helper()
	source := e2eAwaitGitHubSourceRuntime(t, "application source runtime", application.builtSources)
	awaitSignal(t, "application Linux inventory", application.provider.refreshed)
	awaitSignal(t, "application Windows inventory", application.windows.refreshed)
	application.tickers.cycle(t, "membership")
	e2eReconcileApplicationSourceAfterMembership(t, application, source)
	for _, runnerPool := range []string{"linux", "windows"} {
		application.awaitRegistration(t, runnerPool)
	}
	for _, runnerPool := range []string{"linux", "windows"} {
		awaitSignal(t, "application session for "+runnerPool, application.host.sessionOK[runnerPool])
	}
	e2eInstallPublishedApplicationSessions(t, application, source)
	for _, runnerPool := range []string{"linux", "windows"} {
		awaitSignal(t, "application poll for "+runnerPool, application.host.sessions[runnerPool].pollRequested)
	}
}

func (application *e2eApplication) awaitRegistration(t *testing.T, runnerPool string) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case <-application.host.registrationOK[runnerPool]:
		return
	case <-timer.C:
		repositories, clientCalls, ensures := application.host.callSummary()
		t.Fatalf("timed out waiting for application registration for %s; discovered repository counts=%v clients=%d ensures=%v",
			runnerPool, repositories, clientCalls, ensures)
	}
}

func (application *e2eApplication) startShutdownPolicyJobs(
	t *testing.T,
) (linuxStart execution.StartRequest, windowsStart execution.StartRequest) {
	t.Helper()
	queueTime := time.Now().Add(-time.Hour)
	application.deliver(t, "linux", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 901, QueueTime: queueTime,
		}}},
	})
	application.deliver(t, "windows", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 902, QueueTime: queueTime,
		}}},
	})
	linuxStart = awaitProviderStart(t, "application Linux start", application.provider.starts)
	windowsStart = awaitProviderStart(t, "application Windows start", application.windows.starts)
	application.deliver(t, "windows", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(windowsStart.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 902}}},
	})
	application.tickers.cycle(t, "execution")
	return linuxStart, windowsStart
}

func (application *e2eApplication) deliver(
	t *testing.T,
	runnerPool string,
	message *upstream.RunnerScaleSetMessage,
) {
	t.Helper()
	application.deliverWithSourceTicker(t, runnerPool, message, application.tickers.latest(t, "source"))
}

func (application *e2eApplication) deliverWithSourceTicker(
	t *testing.T,
	runnerPool string,
	message *upstream.RunnerScaleSetMessage,
	sourceTicker *e2eManualTicker,
) {
	t.Helper()
	session := application.host.sessions[runnerPool]
	messageID := session.assignMessageID(message)
	acknowledged := session.acknowledgement(messageID)
	pollCompleted := session.pollCompletion(messageID)
	deliveryTimer := time.NewTimer(e2eWaitLimit)
	select {
	case session.messages <- message:
		deliveryTimer.Stop()
	case <-deliveryTimer.C:
		t.Fatalf("timed out waiting for application %s poll to receive message %d", runnerPool, messageID)
	}
	acknowledgementTimer := time.NewTimer(e2eWaitLimit)
	defer acknowledgementTimer.Stop()
	select {
	case <-acknowledged:
	case <-acknowledgementTimer.C:
		processing, processingErr := application.runtime.state.store.ProcessingByPool(context.Background(), core.RunnerPool(runnerPool))
		t.Fatalf("timed out waiting for application %s message %d acknowledgement; queued_messages=%d ack_attempts=%d processing=%+v processing_error=%v logs=%+v",
			runnerPool, messageID, len(session.messages), len(session.ackAttempts), processing, processingErr,
			e2eApplicationMessageLogs(application.logs.records(t), runnerPool, messageID))
	}
	awaitSignal(t, fmt.Sprintf("application %s message %d poll completion", runnerPool, messageID), pollCompleted)
	application.tickers.cycleTicker(t, sourceTicker, "application source maintenance")
}

func (application *e2eApplication) assertShutdownPolicyState(
	t *testing.T,
	policy ActiveRunnerPolicy,
	linux execution.StartRequest,
	windows execution.StartRequest,
) {
	t.Helper()
	store, err := sqlite.Open(context.Background(), application.databasePath)
	if err != nil {
		t.Fatalf("reopen %s shutdown state: %v", policy, err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close %s shutdown state: %v", policy, closeErr)
		}
	}()
	linuxProcessing, linuxErr := store.ProcessingByPool(context.Background(), "linux")
	windowsProcessing, windowsErr := store.ProcessingByPool(context.Background(), "windows")
	if policy == LeaveActiveRunners {
		application.assertLeaveShutdownState(t, linux, windows, linuxProcessing, windowsProcessing, linuxErr, windowsErr)
		return
	}
	application.assertTerminateShutdownState(t, linux, windows, linuxProcessing, windowsProcessing, linuxErr, windowsErr)
}

func (application *e2eApplication) assertLeaveShutdownState(
	t *testing.T,
	linux execution.StartRequest,
	windows execution.StartRequest,
	linuxProcessing []core.ProcessingJob,
	windowsProcessing []core.ProcessingJob,
	linuxErr error,
	windowsErr error,
) {
	t.Helper()
	if application.provider.destroyCount() != 0 || application.windows.destroyCount() != 0 ||
		len(linuxProcessing) != 1 || linuxProcessing[0].Job.ID != linux.JobID ||
		len(windowsProcessing) != 1 || windowsProcessing[0].Job.ID != windows.JobID || !windowsProcessing[0].RunnerStarted {
		t.Fatalf("leave shutdown state: linux=%+v windows=%+v errors=(%v,%v) destroys=(%d,%d)",
			linuxProcessing, windowsProcessing, linuxErr, windowsErr,
			application.provider.destroyCount(), application.windows.destroyCount())
	}
}

func (application *e2eApplication) assertTerminateShutdownState(
	t *testing.T,
	linux execution.StartRequest,
	windows execution.StartRequest,
	linuxProcessing []core.ProcessingJob,
	windowsProcessing []core.ProcessingJob,
	linuxErr error,
	windowsErr error,
) {
	t.Helper()
	if linuxErr != nil || windowsErr != nil || len(linuxProcessing) != 1 ||
		linuxProcessing[0].Job.ID != linux.JobID || linuxProcessing[0].RunnerStarted ||
		len(windowsProcessing) != 1 ||
		windowsProcessing[0].Job.ID != windows.JobID || !windowsProcessing[0].RunnerStarted ||
		application.provider.destroyCount() != 0 || application.windows.destroyCount() != 1 {
		t.Fatalf("terminate shutdown state: linux=%+v windows=%+v errors=(%v,%v) destroys=(%d,%d)",
			linuxProcessing, windowsProcessing, linuxErr, windowsErr,
			application.provider.destroyCount(), application.windows.destroyCount())
	}
}
