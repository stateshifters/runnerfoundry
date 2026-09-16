//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

type e2eReloadHandoff struct {
	application    *e2eApplication
	running        e2eApplicationRun
	deactivation   *e2eReloadGate
	retiredTickers []*e2eManualTicker
}

func e2eBeginReloadHandoff(t *testing.T) e2eReloadHandoff {
	t.Helper()
	application := newE2EReloadApplication(t, LeaveActiveRunners)
	deactivation := newE2EReloadGate()
	application.replacementProvider.setInspectionError(errors.New("scripted replacement inventory preparation gate"))
	application.generationDecorator = e2eGateFirstGenerationDeactivation(deactivation)
	running := e2eStartReloadApplication(t, application)
	retiredTickers := e2eApplicationGenerationTickers(t, application.tickers)
	application.provider.setInspectionError(errors.New("scripted old generation inventory outage"))
	application.tickers.cycle(t, "execution")
	application.deliver(t, "linux", e2eReloadAvailableMessage(1101, application.now()))
	e2eBeginApplicationReload(t, application, 2)
	awaitSignal(t, "old generation deactivation gate", deactivation.started)
	return e2eReloadHandoff{
		application: application, running: running, deactivation: deactivation, retiredTickers: retiredTickers,
	}
}

func e2eGateFirstGenerationDeactivation(deactivation *e2eReloadGate) func(int, generation) generation {
	return func(buildNumber int, built generation) generation {
		if buildNumber != 0 {
			return built
		}
		return &e2eReloadGeneration{generation: built, deactivationGate: deactivation}
	}
}

func e2eAssertReloadHandoffHasNoOverlappingClaims(t *testing.T, handoff e2eReloadHandoff) {
	t.Helper()
	if oldAttempts := handoff.application.provider.attemptCount(); oldAttempts != 0 {
		t.Fatalf("old generation provider attempts before deactivation = %d, want 0", oldAttempts)
	}
	if replacementAttempts := handoff.application.replacementProvider.attemptCount(); replacementAttempts != 0 {
		t.Fatalf("replacement provider attempts before old generation joined = %d, want 0", replacementAttempts)
	}
}

func e2eCompleteReloadHandoff(t *testing.T, handoff e2eReloadHandoff) {
	t.Helper()
	handoff.deactivation.release()
	handoff.application.awaitReady(t, "application reload completion")
	handoff.application.tickers.awaitInitialGeneration(t)
	replacementSource := e2eAwaitGitHubSourceRuntime(
		t, "replacement source runtime", handoff.application.builtSources,
	)
	e2eEstablishReloadSource(t, handoff.application, replacementSource,
		handoff.application.replacementProvider, handoff.application.replacementWindows,
		handoff.application.replacementHost)
	handoff.application.replacementProvider.setInspectionError(nil)
	handoff.application.tickers.cycle(t, "execution")
	replacementStart := e2eAwaitReloadProviderStart(t, handoff.application)
	e2eAssertApplicationGenerationJoined(t, handoff.application, handoff.retiredTickers)
	e2eAssertApplicationUnstartedProcessing(t, handoff.application, replacementStart.JobID)
	e2eStopReloadApplication(t, handoff.running)
}

func e2eEstablishReloadSource(
	t *testing.T,
	application *e2eApplication,
	source *githubSourceRuntime,
	linuxProvider *e2eProvider,
	windowsProvider *e2eProvider,
	host *e2eGitHost,
) {
	t.Helper()
	awaitSignal(t, "application Linux inventory", linuxProvider.refreshed)
	awaitSignal(t, "application Windows inventory", windowsProvider.refreshed)
	application.tickers.cycle(t, "membership")
	e2eReconcileApplicationSourceAfterMembership(t, application, source)
	for _, runnerPool := range []string{"linux", "windows"} {
		awaitSignal(t, "application registration for "+runnerPool, host.registrationOK[runnerPool])
	}
	for _, runnerPool := range []string{"linux", "windows"} {
		awaitSignal(t, "application session for "+runnerPool, host.sessionOK[runnerPool])
	}
	e2eInstallPublishedApplicationSessions(t, application, source)
	for _, runnerPool := range []string{"linux", "windows"} {
		awaitSignal(t, "application poll for "+runnerPool, host.sessions[runnerPool].pollRequested)
	}
}

func e2eReconcileApplicationSourceAfterMembership(
	t *testing.T,
	application *e2eApplication,
	source *githubSourceRuntime,
) {
	t.Helper()
	e2eRunApplicationSourceAtNextDiscovery(t, application, source.discoveryInterval)
	if len(source.desired) != 0 {
		return
	}
	if !source.term.discoveryActive {
		e2eRunApplicationSourceAtNextDiscovery(t, application, source.discoveryInterval)
	}
	e2eAwaitPublishedApplicationDiscovery(t, source, "initial source reconciliation")
	application.tickers.cycle(t, "source")
	if len(source.desired) != 0 {
		return
	}
	e2eRunApplicationSourceAtNextDiscovery(t, application, source.discoveryInterval)
	e2eAwaitPublishedApplicationDiscovery(t, source, "post-membership source recovery")
	application.tickers.cycle(t, "source")
	if len(source.desired) == 0 {
		t.Fatal("post-membership application source reconciliation produced no desired registrations")
	}
}

func e2eRunApplicationSourceAtNextDiscovery(
	t *testing.T,
	application *e2eApplication,
	discoveryInterval time.Duration,
) {
	t.Helper()
	application.advanceClock(discoveryInterval)
	application.tickers.cycle(t, "membership")
	application.tickers.cycle(t, "source")
}

func e2eAwaitPublishedApplicationDiscovery(t *testing.T, source *githubSourceRuntime, description string) {
	t.Helper()
	if source.term == nil {
		t.Fatal("application source leadership term is not active")
	}
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case result := <-source.term.discoveryResult:
		source.term.discoveryResult <- result
	case <-timer.C:
		t.Fatalf("timed out waiting for %s publication", description)
	}
}

func e2eInstallPublishedApplicationSessions(
	t *testing.T,
	application *e2eApplication,
	source *githubSourceRuntime,
) {
	t.Helper()
	missingResults := len(source.desired) - len(source.sessions)
	results := make([]githubRegistrationOpenResult, 0, missingResults)
	for index := 0; index < missingResults; index++ {
		results = append(results, e2eAwaitPublishedSessionOpenResult(t, source.term, index+1, missingResults))
	}
	for _, result := range results {
		source.term.openResults <- result
		application.tickers.cycle(t, "source")
	}
}

func e2eAwaitGitHubSourceRuntime(
	t *testing.T,
	description string,
	sources <-chan e2eGitHubSourceRuntimeResult,
) *githubSourceRuntime {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case result := <-sources:
		if result.err != nil {
			t.Fatalf("load %s: %v", description, result.err)
		}
		return result.source
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

func e2eAwaitReloadProviderStart(t *testing.T, application *e2eApplication) execution.StartRequest {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case started := <-application.replacementProvider.starts:
		return started
	case <-timer.C:
		processing, processingErr := application.runtime.state.store.processing.ProcessingByPool(context.Background(), "linux")
		application.runtime.reporter.mutex.Lock()
		failures := fmt.Sprintf("%+v", application.runtime.reporter.failures)
		status := application.runtime.reporter.lastStatus
		application.runtime.reporter.mutex.Unlock()
		t.Fatalf("timed out waiting for replacement generation provider start; old_attempts=%d replacement_attempts=%d processing=%+v processing_error=%v failures=%+v status=%+v",
			application.provider.attemptCount(), application.replacementProvider.attemptCount(), processing,
			processingErr, failures, status)
		return execution.StartRequest{}
	}
}

type e2eInventoryEvidencePreparation func(*testing.T, *e2eApplication)

func e2eRetainCurrentInventoryEvidence(t *testing.T, _ *e2eApplication) {
	t.Helper()
}

func e2eRefreshInventoryEvidence(t *testing.T, application *e2eApplication) {
	t.Helper()
	application.tickers.cycle(t, "execution")
}

func e2eExerciseReloadPolicy(
	t *testing.T,
	policy ActiveRunnerPolicy,
	wantDestroys int,
	prepareEvidence e2eInventoryEvidencePreparation,
) {
	t.Helper()
	application := newE2EReloadApplication(t, policy)
	application.replacementHost = nil
	application.replacementProvider = nil
	application.replacementWindows = nil
	finalizerRequests := make(chan []core.RunnerPool, 1)
	application.generationDecorator = e2eObserveFirstGenerationFinalizers(finalizerRequests)
	running := e2eStartReloadApplication(t, application)
	e2eAssertReloadPolicyConfiguration(t, application, policy)
	retiredTickers := e2eApplicationGenerationTickers(t, application.tickers)
	started := e2eStartAcceptedWindowsJob(t, application, 1301)
	prepareEvidence(t, application)
	e2eRequestRunnerPoolRemovalReload(t, application, "windows")
	e2eAssertTickersStopped(t, retiredTickers)
	e2eAssertReloadFinalizerSelection(t, policy, finalizerRequests)
	e2eAssertReloadPolicyOutcome(t, application, policy, started, wantDestroys)
	e2eStopReloadApplication(t, running)
}

func e2eObserveFirstGenerationFinalizers(requests chan<- []core.RunnerPool) func(int, generation) generation {
	return func(buildNumber int, built generation) generation {
		if buildNumber != 0 {
			return built
		}
		return &e2eReloadGeneration{generation: built, finalizerRequests: requests}
	}
}

func e2eAssertReloadPolicyConfiguration(
	t *testing.T,
	application *e2eApplication,
	want ActiveRunnerPolicy,
) {
	t.Helper()
	if observed := awaitApplicationPolicy(t, application.observedPolicy); observed != want {
		t.Fatalf("reload supervisor policy = %q, want %q", observed, want)
	}
}

type e2eInFlightRunnerLifecycle struct {
	application *e2eApplication
	running     e2eApplicationRun
	attempt     execution.StartRequest
}

func e2eReloadWithInFlightRunner(t *testing.T) e2eInFlightRunnerLifecycle {
	t.Helper()
	application := newE2EReloadApplication(t, LeaveActiveRunners)
	application.replacementHost = nil
	application.replacementProvider = nil
	application.replacementWindows = nil
	startGate := application.provider.blockNextStart(nil)
	running := e2eStartReloadApplication(t, application)
	application.deliver(t, "linux", e2eReloadAvailableMessage(1401, application.now()))
	attempt := awaitProviderStart(t, "in-flight provider start", application.provider.attempts)
	awaitSignal(t, "in-flight provider boundary", startGate.started)
	e2eRequestApplicationReload(t, application, 2)
	application.tickers.awaitInitialGeneration(t)
	e2eAssertSingleProviderAttempt(t, application, "leave reload")
	e2eAssertApplicationUnstartedProcessing(t, application, attempt.JobID)
	return e2eInFlightRunnerLifecycle{
		application: application, running: running, attempt: attempt,
	}
}

func e2eObserveIndependentRunnerLifecycle(t *testing.T, lifecycle e2eInFlightRunnerLifecycle) {
	t.Helper()
	lifecycle.application.provider.recordRunningInstance(lifecycle.attempt)
	replacementSource := e2eAwaitGitHubSourceRuntime(
		t, "in-flight replacement source", lifecycle.application.builtSources,
	)
	e2eEstablishReloadSource(t, lifecycle.application, replacementSource,
		lifecycle.application.provider, lifecycle.application.windows, lifecycle.application.host)
	lifecycle.application.deliver(t, "linux", e2eRunnerStartedMessage(1401, lifecycle.attempt))
	e2eAssertApplicationStartedProcessing(t, lifecycle.application, lifecycle.attempt.JobID)
	e2eAssertSingleProviderAttempt(t, lifecycle.application, "independent runner acceptance")
	lifecycle.application.deliver(t, "linux", e2eRunnerCompletedMessage(1401, lifecycle.attempt))
	lifecycle.application.tickers.cycle(t, "execution")
}

func e2eRunnerStartedMessage(
	requestID int64,
	attempt execution.StartRequest,
) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(attempt.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	}
}

func e2eRunnerCompletedMessage(
	requestID int64,
	attempt execution.StartRequest,
) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(attempt.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	}
}

func e2eAssertSingleProviderAttempt(t *testing.T, application *e2eApplication, phase string) {
	t.Helper()
	if attempts := application.provider.attemptCount(); attempts != 1 {
		t.Fatalf("provider attempts after %s = %d, want 1", phase, attempts)
	}
}

func e2eAssertIndependentRunnerCleanup(t *testing.T, lifecycle e2eInFlightRunnerLifecycle) {
	t.Helper()
	processing, err := lifecycle.application.runtime.state.store.processing.ProcessingByPool(context.Background(), "linux")
	destroys := lifecycle.application.provider.destroyCount()
	attempts := lifecycle.application.provider.attemptCount()
	if err != nil || len(processing) != 0 || destroys != 1 || attempts != 1 {
		t.Fatalf("completed in-flight runner cleanup processing=%+v error=%v destroys=%d attempts=%d",
			processing, err, destroys, attempts)
	}
	e2eStopReloadApplication(t, lifecycle.running)
}
