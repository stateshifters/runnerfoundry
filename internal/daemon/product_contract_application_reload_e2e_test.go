//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestProductContractApplicationReloadJoinsOldGenerationBeforeReplacementClaims(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	handoff := e2eBeginReloadHandoff(t)
	e2eAssertReloadHandoffHasNoOverlappingClaims(t, handoff)
	e2eCompleteReloadHandoff(t, handoff)
}

func TestProductContractApplicationReloadFailureKeepsOldGenerationAvailable(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	for _, failure := range []string{"preparation", "activation"} {
		t.Run(failure, func(t *testing.T) {
			application := newE2EReloadApplication(t, LeaveActiveRunners)
			application.replacementHost = nil
			application.replacementProvider = nil
			application.replacementWindows = nil
			if failure == "preparation" {
				application.replacementProviderValidationErr = errors.New("scripted replacement provider validation failure")
			} else {
				application.generationDecorator = e2eReloadActivationFailureDecorator()
			}
			running := e2eStartReloadApplication(t, application)
			oldSourceTicker := application.tickers.latest(t, "source")
			awaitSignal(t, "initial scripted provider validation", application.provider.validated)

			e2eRequestRejectedApplicationReload(t, application, 2)
			awaitSignal(t, "replacement scripted provider validation", application.provider.validated)
			if failure == "activation" {
				e2eAssertTickersStopped(t, e2eApplicationGenerationTickers(t, application.tickers))
			}
			application.tickers.cycleTicker(t, oldSourceTicker, "reactivated old source maintenance")
			application.deliverWithSourceTicker(
				t, "linux", e2eReloadAvailableMessage(1201, application.now()), oldSourceTicker,
			)
			awaitProviderStart(t, "old generation provider after rejected reload", application.provider.starts)
			e2eStopReloadApplication(t, running)
		})
	}
}

func TestProductContractApplicationReloadRetriesOldGenerationClose(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	application := newE2EApplication(t)
	closeFailure := &e2eRetryableGenerationCloser{remaining: []error{context.DeadlineExceeded}}
	application.generationDecorator = func(buildNumber int, built generation) generation {
		if buildNumber != 0 {
			return built
		}
		e2eAppendProductionGenerationCloser(t, built, closeFailure)
		return built
	}
	running := e2eStartReloadApplicationWithoutSource(t, application)
	oldTickers := e2eApplicationGenerationTickers(t, application.tickers)

	e2eRequestApplicationReload(t, application, 2)
	if calls := closeFailure.callCount(); calls != 1 {
		t.Fatalf("old generation close calls after timeout = %d, want 1", calls)
	}
	e2eAssertTickersStopped(t, oldTickers)
	e2eRequestApplicationReload(t, application, 3)
	if calls := closeFailure.callCount(); calls != 2 {
		t.Fatalf("old generation close calls after retry = %d, want 2", calls)
	}
	e2eStopReloadApplication(t, running)
}

func TestProductContractApplicationReloadAppliesActiveRunnerPolicy(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	t.Run("leave retains observed runner", func(t *testing.T) {
		e2eExerciseReloadPolicy(t, LeaveActiveRunners, 0, e2eRetainCurrentInventoryEvidence)
	})
	t.Run("terminate retains unobserved staggler", func(t *testing.T) {
		e2eExerciseReloadPolicy(t, TerminateActiveRunners, 0, e2eRetainCurrentInventoryEvidence)
	})
	t.Run("terminate destroys observed runner", func(t *testing.T) {
		e2eExerciseReloadPolicy(t, TerminateActiveRunners, 1, e2eRefreshInventoryEvidence)
	})
}

func TestProductContractApplicationLeaveReloadRetainsInFlightRunnerLifecycle(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	lifecycle := e2eReloadWithInFlightRunner(t)
	e2eObserveIndependentRunnerLifecycle(t, lifecycle)
	e2eAssertIndependentRunnerCleanup(t, lifecycle)
}

func TestProductContractApplicationTerminateReloadRetainsCleanupFailure(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	application := newE2EReloadApplication(t, TerminateActiveRunners)
	application.replacementHost = nil
	application.replacementProvider = nil
	application.replacementWindows = nil
	running := e2eStartReloadApplication(t, application)
	started := e2eStartAcceptedWindowsJob(t, application, 1501)
	application.tickers.cycle(t, "execution")
	application.windows.setDestroyError(errors.New("scripted terminate cleanup failure"))

	e2eRequestRunnerPoolRemovalReload(t, application, "windows")
	application.tickers.awaitInitialGeneration(t)
	e2eAssertReloadPolicyOutcome(t, application, TerminateActiveRunners, started, 0)
	if attempts := application.windows.attemptCount(); attempts != 1 {
		t.Fatalf("Windows provider attempts after failed terminate cleanup = %d, want 1", attempts)
	}

	application.windows.setDestroyError(nil)
	e2eRequestApplicationReload(t, application, 2)
	application.tickers.awaitInitialGeneration(t)
	e2eAssertReloadPolicyOutcome(t, application, TerminateActiveRunners, started, 1)
	if attempts := application.windows.attemptCount(); attempts != 1 {
		t.Fatalf("Windows provider attempts after successful cleanup retry = %d, want 1", attempts)
	}
	e2eStopReloadApplication(t, running)
}
