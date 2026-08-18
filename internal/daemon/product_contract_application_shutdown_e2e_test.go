//go:build e2e

package daemon

import (
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/execution"
)

func TestProductContractApplicationLeaveShutdownBudget(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	shutdown := startE2ETimedOutLeaveShutdown(t)
	shutdown.expire(t)
	shutdown.assertDurableEffects(t)
	shutdown.finishFixtureCleanup(t)
}

type e2eTimedOutLeaveShutdown struct {
	application       *e2eApplication
	running           e2eApplicationRun
	closeStarted      <-chan struct{}
	releaseClose      func()
	timerRequested    <-chan time.Duration
	timerExpired      chan<- time.Time
	linux             execution.StartRequest
	windows           execution.StartRequest
	generationTickers []*e2eManualTicker
}

func startE2ETimedOutLeaveShutdown(t *testing.T) e2eTimedOutLeaveShutdown {
	t.Helper()
	application := newE2EApplication(t)
	application.enableSource(LeaveActiveRunners)
	for index := range application.configured.RunnerPools {
		application.configured.RunnerPools[index].MaxRunners = 2
	}
	closeStarted, releaseClose := application.host.sessions["linux"].blockClose()
	timerRequested := make(chan time.Duration, 1)
	timerExpired := make(chan time.Time, 1)
	application.shutdownTimerFactory = func(duration time.Duration) shutdownTimer {
		timerRequested <- duration
		return shutdownTimer{expired: timerExpired, stop: func() {}}
	}
	running := startE2EApplication(t, application)
	t.Cleanup(releaseClose)
	application.awaitReady(t, "bounded-shutdown application readiness")
	awaitConfiguredBuild(t, application.builtConfigs)
	application.tickers.awaitInitialGeneration(t)
	application.establishSource(t)
	linux, windows := application.startShutdownPolicyJobs(t)
	return e2eTimedOutLeaveShutdown{
		application: application, running: running, closeStarted: closeStarted, releaseClose: releaseClose,
		timerRequested: timerRequested, timerExpired: timerExpired, linux: linux, windows: windows,
		generationTickers: e2eApplicationGenerationTickers(t, application.tickers),
	}
}

func (shutdown e2eTimedOutLeaveShutdown) expire(t *testing.T) {
	t.Helper()
	shutdown.running.cancel()
	awaitSignal(t, "blocked production session close", shutdown.closeStarted)
	if duration := awaitShutdownTimerDuration(t, shutdown.timerRequested); duration != leaveShutdownTimeout {
		t.Fatalf("leave shutdown timer = %s, want %s", duration, leaveShutdownTimeout)
	}
	shutdown.timerExpired <- time.Now()
	if err := awaitApplicationResult(t, shutdown.running.result); err != nil {
		t.Fatalf("bounded leave shutdown: %v", err)
	}
}

func (shutdown e2eTimedOutLeaveShutdown) assertDurableEffects(t *testing.T) {
	t.Helper()
	e2eAssertTickersStopped(t, shutdown.generationTickers)
	shutdown.application.assertTimedOutLeaveShutdownState(t, shutdown.linux, shutdown.windows)
	shutdown.application.assertCapacityAtCutoff(t, shutdown.application.now().Add(-runtimeMemberTTL), 1)
	shutdown.application.assertCapacityAtCutoff(t, shutdown.application.now().Add(time.Second), 0)
	select {
	case <-shutdown.application.runtimeClosed:
		t.Fatal("leave shutdown timeout unexpectedly completed blocked process-local cleanup")
	default:
	}
}

func (shutdown e2eTimedOutLeaveShutdown) finishFixtureCleanup(t *testing.T) {
	t.Helper()
	// Releasing the scripted session is fixture teardown. The product evidence
	// above is deliberately captured before the blocked cleanup can continue.
	shutdown.releaseClose()
	awaitSignal(t, "timed-out runtime fixture cleanup", shutdown.application.runtimeClosed)
	shutdown.application.assertLeaveShutdownRestartReconciliation(t, shutdown.linux, shutdown.windows)
}
