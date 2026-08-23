//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

type e2eReloadGeneration struct {
	generation
	activateErr       error
	deactivationGate  *e2eReloadGate
	finalizerRequests chan<- []core.RunnerPool
}

func (generation *e2eReloadGeneration) Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer {
	if generation.finalizerRequests != nil {
		generation.finalizerRequests <- append([]core.RunnerPool(nil), runnerPools...)
	}
	return generation.generation.Finalizers(runnerPools)
}

type e2eReloadGate struct {
	started     chan struct{}
	releaseGate chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func newE2EReloadGate() *e2eReloadGate {
	return &e2eReloadGate{started: make(chan struct{}), releaseGate: make(chan struct{})}
}

func (gate *e2eReloadGate) wait(ctx context.Context) error {
	gate.startOnce.Do(func() { close(gate.started) })
	select {
	case <-gate.releaseGate:
		return nil
	case <-ctx.Done():
		return errors.Join(errors.New("wait for scripted reload gate"), ctx.Err())
	}
}

func (gate *e2eReloadGate) release() {
	gate.releaseOnce.Do(func() { close(gate.releaseGate) })
}

func (generation *e2eReloadGeneration) Prepare(ctx context.Context) error {
	if err := generation.generation.Prepare(ctx); err != nil {
		return fmt.Errorf("prepare delegated reload generation: %w", err)
	}
	return nil
}

func (generation *e2eReloadGeneration) Activate(ctx context.Context) error {
	if err := generation.generation.Activate(ctx); err != nil {
		return fmt.Errorf("activate delegated reload generation: %w", err)
	}
	if generation.activateErr != nil {
		return generation.activateErr
	}
	return nil
}

func (generation *e2eReloadGeneration) Deactivate(ctx context.Context) error {
	if generation.deactivationGate != nil {
		if err := generation.deactivationGate.wait(ctx); err != nil {
			return err
		}
	}
	if err := generation.generation.Deactivate(ctx); err != nil {
		return fmt.Errorf("deactivate delegated reload generation: %w", err)
	}
	return nil
}

func (generation *e2eReloadGeneration) Close(ctx context.Context) error {
	if err := generation.generation.Close(ctx); err != nil {
		return fmt.Errorf("close delegated reload generation: %w", err)
	}
	return nil
}

type e2eRetryableGenerationCloser struct {
	mutex     sync.Mutex
	remaining []error
	calls     int
}

func (closer *e2eRetryableGenerationCloser) Close(context.Context) error {
	closer.mutex.Lock()
	defer closer.mutex.Unlock()
	closer.calls++
	if len(closer.remaining) == 0 {
		return nil
	}
	err := closer.remaining[0]
	closer.remaining = closer.remaining[1:]
	return err
}

func (closer *e2eRetryableGenerationCloser) callCount() int {
	closer.mutex.Lock()
	defer closer.mutex.Unlock()
	return closer.calls
}

func e2eAppendProductionGenerationCloser(
	t *testing.T,
	built generation,
	closer runtimeCloser,
) {
	t.Helper()
	runtimeGeneration, validRuntimeGeneration := built.(*runtimeGeneration)
	if !validRuntimeGeneration {
		t.Fatalf("production generation type = %T, want *runtimeGeneration", built)
	}
	maintenance, validMaintenance := runtimeGeneration.generation.(*maintenanceGeneration)
	if !validMaintenance {
		t.Fatalf("production runtime generation type = %T, want *maintenanceGeneration", runtimeGeneration.generation)
	}
	maintenance.closers = append(maintenance.closers, closer)
	maintenance.closerClosed = append(maintenance.closerClosed, false)
}

func newE2EReloadApplication(t *testing.T, policy ActiveRunnerPolicy) *e2eApplication {
	t.Helper()
	application := newE2EApplication(t)
	application.enableSource(policy)
	application.replacementHost = newE2EGitHost()
	application.replacementProvider = newE2EProviderForEndpoint("e2e:reload-replacement-linux")
	application.replacementWindows = newE2EProviderForEndpoint("e2e:reload-replacement-windows")
	return application
}

func e2eStartReloadApplication(t *testing.T, application *e2eApplication) e2eApplicationRun {
	t.Helper()
	running := e2eStartReloadApplicationWithoutSource(t, application)
	source := e2eAwaitGitHubSourceRuntime(t, "initial source runtime", application.builtSources)
	e2eEstablishReloadSource(t, application, source, application.provider, application.windows, application.host)
	return running
}

type e2eGitHubSourceRuntimeResult struct {
	source *githubSourceRuntime
	err    error
}

func e2eGitHubSourceRuntime(maintenance *maintenanceGeneration) e2eGitHubSourceRuntimeResult {
	source, err := githubSourceFromMaintenance(maintenance)
	if err != nil {
		return e2eGitHubSourceRuntimeResult{err: err}
	}
	return e2eGitHubSourceRuntimeResult{source: source}
}

func e2eStartReloadApplicationWithoutSource(t *testing.T, application *e2eApplication) e2eApplicationRun {
	t.Helper()
	running := startE2EApplication(t, application)
	application.awaitReady(t, "reload application readiness")
	awaitConfiguredBuild(t, application.builtConfigs)
	application.tickers.awaitInitialGeneration(t)
	return running
}

func e2eStopReloadApplication(t *testing.T, running e2eApplicationRun) {
	t.Helper()
	running.cancel()
	if err := awaitApplicationResult(t, running.result); err != nil {
		t.Fatalf("stop reload application: %v", err)
	}
}

func e2eReloadActivationFailureDecorator() func(int, generation) generation {
	return func(buildNumber int, built generation) generation {
		if buildNumber != 1 {
			return built
		}
		return &e2eReloadGeneration{
			generation:  built,
			activateErr: errors.New("scripted replacement activation failure"),
		}
	}
}

func e2eRequestApplicationReload(t *testing.T, application *e2eApplication, maxRunners int) {
	t.Helper()
	e2eBeginApplicationReload(t, application, maxRunners)
	application.awaitReady(t, "application reload completion")
}

func e2eBeginApplicationReload(t *testing.T, application *e2eApplication, maxRunners int) {
	t.Helper()
	application.mutex.Lock()
	application.configured.RunnerPools[0].MaxRunners = maxRunners
	application.mutex.Unlock()
	application.reloads <- struct{}{}
	application.awaitReloading(t, "application reload notification")
	awaitConfiguredBuild(t, application.builtConfigs)
}

func e2eRequestRejectedApplicationReload(t *testing.T, application *e2eApplication, maxRunners int) {
	t.Helper()
	e2eRequestApplicationReload(t, application, maxRunners)
}

func e2eRequestRunnerPoolRemovalReload(
	t *testing.T,
	application *e2eApplication,
	removed core.RunnerPool,
) {
	t.Helper()
	application.mutex.Lock()
	retained := make([]config.RunnerPoolConfig, 0, len(application.configured.RunnerPools)-1)
	for _, runnerPool := range application.configured.RunnerPools {
		if core.RunnerPool(runnerPool.Name) != removed {
			retained = append(retained, runnerPool)
		}
	}
	application.configured.RunnerPools = retained
	application.mutex.Unlock()
	application.reloads <- struct{}{}
	application.awaitReloading(t, "Runner Pool removal reload notification")
	built := awaitConfiguredBuild(t, application.builtConfigs)
	for _, runnerPool := range built.RunnerPools {
		if core.RunnerPool(runnerPool.Name) == removed {
			t.Fatalf("replacement configuration retained removed Runner Pool %q", removed)
		}
	}
	application.awaitReady(t, "Runner Pool removal reload completion")
}

func e2eAssertReloadFinalizerSelection(
	t *testing.T,
	policy ActiveRunnerPolicy,
	requests <-chan []core.RunnerPool,
) {
	t.Helper()
	if policy == LeaveActiveRunners {
		select {
		case runnerPools := <-requests:
			t.Fatalf("leave reload selected finalizers for Runner Pools %v", runnerPools)
		default:
		}
		return
	}
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	var runnerPools []core.RunnerPool
	select {
	case runnerPools = <-requests:
	case <-timer.C:
		t.Fatal("timed out waiting for terminate reload finalizer selection")
	}
	if len(runnerPools) != 1 || runnerPools[0] != "windows" {
		t.Fatalf("terminate reload selected finalizers for Runner Pools %v, want [windows]", runnerPools)
	}
}

func e2eReloadAvailableMessage(requestID int64, now time.Time) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: now.Add(-time.Hour),
		}}},
	}
}

func e2eStartAcceptedWindowsJob(
	t *testing.T,
	application *e2eApplication,
	requestID int64,
) execution.StartRequest {
	t.Helper()
	application.deliver(t, "windows", e2eReloadAvailableMessage(requestID, application.now()))
	started := awaitProviderStart(t, "active Windows provider start", application.windows.starts)
	application.deliver(t, "windows", &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	return started
}

func e2eAssertReloadPolicyOutcome(
	t *testing.T,
	application *e2eApplication,
	policy ActiveRunnerPolicy,
	started execution.StartRequest,
	wantDestroys int,
) {
	t.Helper()
	processing, err := application.runtime.state.store.ProcessingByPool(context.Background(), "windows")
	if destroys := application.windows.destroyCount(); destroys != wantDestroys {
		instances, inspectErr := application.windows.Inspect(context.Background())
		t.Fatalf("%s reload destroys = %d, want %d; processing=%+v processing_error=%v instances=%+v inspect_error=%v",
			policy, destroys, wantDestroys, processing, err, instances, inspectErr)
	}
	if err != nil || len(processing) != 1 || processing[0].Job.ID != started.JobID || !processing[0].RunnerStarted {
		t.Fatalf("%s reload processing = %+v, error = %v", policy, processing, err)
	}
}

func e2eApplicationGenerationTickers(
	t *testing.T,
	tickers *e2eApplicationTickers,
) []*e2eManualTicker {
	t.Helper()
	return []*e2eManualTicker{
		tickers.latest(t, "membership"), tickers.latest(t, "source"), tickers.latest(t, "execution"),
	}
}

func e2eAssertApplicationGenerationJoined(
	t *testing.T,
	application *e2eApplication,
	tickers []*e2eManualTicker,
) {
	t.Helper()
	e2eAssertTickersStopped(t, tickers)
	awaitSignal(t, "old source session close", application.host.sessions["linux"].closed)
	awaitSignal(t, "replacement source session", application.replacementHost.sessionOK["linux"])
	if application.provider.attemptCount() != 0 || application.replacementProvider.attemptCount() != 1 {
		t.Fatalf("reload provider attempts old=%d new=%d, want old=0 new=1",
			application.provider.attemptCount(), application.replacementProvider.attemptCount())
	}
}

func e2eAssertTickersStopped(t *testing.T, tickers []*e2eManualTicker) {
	t.Helper()
	for _, ticker := range tickers {
		awaitSignal(t, "retired generation ticker stop", ticker.stopped)
	}
}

func e2eAssertTickersRunning(t *testing.T, tickers []*e2eManualTicker) {
	t.Helper()
	for _, ticker := range tickers {
		select {
		case <-ticker.stopped:
			t.Fatal("retired generation ticker stopped before close retry succeeded")
		default:
		}
	}
}

func e2eAssertApplicationUnstartedProcessing(
	t *testing.T,
	application *e2eApplication,
	jobID core.JobID,
) {
	t.Helper()
	processing := e2eLoadSingleApplicationProcessing(t, application, jobID)
	if processing.RunnerStarted {
		t.Fatalf("reload processing unexpectedly records runner started: %+v", processing)
	}
}

func e2eAssertApplicationStartedProcessing(
	t *testing.T,
	application *e2eApplication,
	jobID core.JobID,
) {
	t.Helper()
	processing := e2eLoadSingleApplicationProcessing(t, application, jobID)
	if !processing.RunnerStarted {
		t.Fatalf("reload processing does not record runner started: %+v", processing)
	}
}

func e2eLoadSingleApplicationProcessing(
	t *testing.T,
	application *e2eApplication,
	jobID core.JobID,
) core.ProcessingJob {
	t.Helper()
	processing, err := application.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].Job.ID != jobID {
		t.Fatalf("reload processing = %+v, error = %v", processing, err)
	}
	return processing[0]
}
