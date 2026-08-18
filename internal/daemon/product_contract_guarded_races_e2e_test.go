//go:build e2e

package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

func TestProductContractRunnerAcceptanceWinsRecoveryRaceSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseRunnerAcceptanceWinsRecoveryRace(t, newSQLiteE2EHarness(t))
}

func TestProductContractRunnerAcceptanceWinsRecoveryRacePostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseRunnerAcceptanceWinsRecoveryRace(t, newPostgresE2EHarness(t))
}

func exerciseRunnerAcceptanceWinsRecoveryRace(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	started := startGuardedRecoveryCandidate(t, harness, 901)
	providerAttemptsBeforeRace := harness.provider.attemptCount()
	barrier := harness.provider.blockNextDestroy(t)

	maintenanceBefore := harness.beginExecutionMaintenance()
	awaitRunnerInstance(t, "expired runner recovery cleanup", barrier.started)
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{
			RunnerName:     string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 901},
		}},
	})
	barrier.Release()
	harness.awaitExecutionMaintenance(maintenanceBefore)

	processing := loadGuardedRaceProcessing(t, harness)
	if len(processing) != 1 || processing[0].Job.ID != started.JobID || !processing[0].RunnerStarted ||
		processing[0].SourceTerminal {
		t.Fatalf("processing after runner-acceptance race = %+v", processing)
	}
	assertNoGuardedRaceReplacement(t, harness, providerAttemptsBeforeRace, 901, core.QueueProcessing)
}

func TestProductContractTerminalObservationWinsRecoveryRaceSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseTerminalObservationWinsRecoveryRace(t, newSQLiteE2EHarness(t))
}

func TestProductContractTerminalObservationWinsRecoveryRacePostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseTerminalObservationWinsRecoveryRace(t, newPostgresE2EHarness(t))
}

func exerciseTerminalObservationWinsRecoveryRace(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	startGuardedRecoveryCandidate(t, harness, 902)
	providerAttemptsBeforeRace := harness.provider.attemptCount()
	barrier := harness.provider.blockNextDestroy(t)

	maintenanceBefore := harness.beginExecutionMaintenance()
	awaitRunnerInstance(t, "expired runner terminal cleanup", barrier.started)
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{
			Result:         "canceled",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 902},
		}},
	})
	barrier.Release()
	harness.awaitExecutionMaintenance(maintenanceBefore)

	processing := loadGuardedRaceProcessing(t, harness)
	if len(processing) != 1 || !processing[0].SourceTerminal || processing[0].RunnerStarted {
		t.Fatalf("processing after terminal-observation race = %+v", processing)
	}
	harness.completeMaintenanceCycle(harness.execution)
	processing = loadGuardedRaceProcessing(t, harness)
	if len(processing) != 0 {
		t.Fatalf("processing after terminal cleanup = %+v", processing)
	}
	assertNoGuardedRaceReplacement(t, harness, providerAttemptsBeforeRace, 902, core.QueueSourceTerminal)
}

func startGuardedRecoveryCandidate(t *testing.T, harness *e2eHarness, requestID int64) execution.StartRequest {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID,
			QueueTime:       harness.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	started := awaitProviderStart(t, "runner before guarded recovery race", harness.provider.starts)
	harness.advanceClock(6 * time.Minute)
	return started
}

func loadGuardedRaceProcessing(t *testing.T, harness *e2eHarness) []core.ProcessingJob {
	t.Helper()
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil {
		t.Fatalf("load processing after guarded recovery race: %v", err)
	}
	return processing
}

func assertNoGuardedRaceReplacement(
	t *testing.T,
	harness *e2eHarness,
	wantProviderAttempts int,
	requestID int64,
	wantQueueOutcome core.QueueOutcome,
) {
	t.Helper()
	if actualProviderAttempts := harness.provider.attemptCount(); actualProviderAttempts != wantProviderAttempts {
		t.Fatalf("provider attempts after guarded recovery race = %d, want %d", actualProviderAttempts, wantProviderAttempts)
	}
	assertExactDemandState(t, harness, requestID, wantQueueOutcome)
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID,
			QueueTime:       harness.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	if actualProviderAttempts := harness.provider.attemptCount(); actualProviderAttempts != wantProviderAttempts {
		t.Fatalf("provider attempts after source redelivery = %d, want %d", actualProviderAttempts, wantProviderAttempts)
	}
}

type e2eProviderDestroyBarrier struct {
	started chan execution.RunnerInstance
	release chan struct{}
	once    sync.Once
}

func (provider *e2eProvider) blockNextDestroy(t *testing.T) *e2eProviderDestroyBarrier {
	t.Helper()
	barrier := &e2eProviderDestroyBarrier{
		started: make(chan execution.RunnerInstance, 1),
		release: make(chan struct{}),
	}
	provider.mutex.Lock()
	provider.destroyBarrier = barrier
	provider.mutex.Unlock()
	t.Cleanup(barrier.Release)
	return barrier
}

func (barrier *e2eProviderDestroyBarrier) Release() {
	barrier.once.Do(func() { close(barrier.release) })
}

func (harness *e2eHarness) beginExecutionMaintenance() uint64 {
	harness.t.Helper()
	barrier := harness.laneCompleted[harness.execution]
	before := barrier.count()
	harness.execution.Tick(harness.currentTime)
	return before
}

func (harness *e2eHarness) awaitExecutionMaintenance(before uint64) {
	harness.t.Helper()
	harness.laneCompleted[harness.execution].waitAfter(
		harness.t,
		"guarded recovery maintenance completion",
		before,
	)
}
