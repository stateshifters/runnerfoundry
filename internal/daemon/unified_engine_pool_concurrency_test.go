package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestInventoryRefreshWaitsForActiveClaimsAndFailsClosed(t *testing.T) {
	gate := newQueuedJobEligibilityGate()
	markQueuedJobPoolsEligible(gate, "linux")
	claim := startBlockedEligibleClaim(gate)
	awaitTestSignal(t, claim.started, "active claim")
	refreshStarted := make(chan struct{})
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- gate.RefreshInventory(func() error {
			close(refreshStarted)
			return nil
		})
	}()
	select {
	case <-refreshStarted:
		t.Fatal("inventory refresh overlapped an active claim")
	case <-time.After(20 * time.Millisecond):
	}
	close(claim.release)
	assertNoTestError(t, <-claim.done, "claim completion")
	assertNoTestError(t, <-refreshDone, "inventory refresh completion")
	_, eligible, err := gate.requestIfEligible(context.Background(), "linux", claim.requester)
	if err != nil || eligible {
		t.Fatalf("post-refresh request = eligible %t, error %v; want fail-closed", eligible, err)
	}
}

type blockedEligibleClaim struct {
	started   <-chan struct{}
	release   chan<- struct{}
	done      <-chan error
	requester queuedJobRequester
}

func startBlockedEligibleClaim(gate *queuedJobEligibilityGate) blockedEligibleClaim {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	requester := queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		close(started)
		<-release
		return runtimeengine.ProcessResult{}, nil
	}}
	go func() {
		_, _, err := gate.requestIfEligible(context.Background(), "linux", requester)
		done <- err
	}()
	return blockedEligibleClaim{started: started, release: release, done: done, requester: requester}
}

func TestExecutionMaintenanceMakesEachPoolEligibleWithoutWaitingForSiblings(t *testing.T) {
	events := make([]string, 0)
	releaseLinux := make(chan struct{})
	windowsReconciled := make(chan struct{})
	woken := make(chan core.RunnerPool, 2)
	engine := unifiedEngineFixture(&events)
	engine.leadership = leadershipFake{leader: false, events: &events}
	engine.jobGate = newQueuedJobEligibilityGate()
	engine.wakeJobs = func(runnerPool core.RunnerPool) { woken <- runnerPool }
	engine.pools = []RunnerPoolRuntime{
		{RunnerPool: "linux", runtime: blockingReconcilePoolRuntime{release: releaseLinux}},
		{RunnerPool: "windows", runtime: signallingReconcilePoolRuntime{reconciled: windowsReconciled}},
	}

	done := make(chan error, 1)
	go func() { done <- engine.MaintainExecution(context.Background()) }()
	awaitTestSignal(t, windowsReconciled, "Windows reconciliation")
	select {
	case runnerPool := <-woken:
		if runnerPool != "windows" {
			t.Fatalf("first eligible Runner Pool = %q, want windows", runnerPool)
		}
	default:
		t.Fatal("Windows was not woken immediately after its reconciliation")
	}
	close(releaseLinux)
	assertNoTestError(t, <-done, "maintain execution")
}

type blockingReconcilePoolRuntime struct {
	release <-chan struct{}
}

func (runtime blockingReconcilePoolRuntime) RequestWork(
	context.Context,
	core.RunnerPool,
) (runtimeengine.ProcessResult, error) {
	return runtimeengine.ProcessResult{}, nil
}

func (runtime blockingReconcilePoolRuntime) Reconcile(context.Context, core.RunnerPool) error {
	<-runtime.release
	return nil
}

func (blockingReconcilePoolRuntime) RecoverForeign(context.Context, core.RunnerPool) error {
	return nil
}

type signallingReconcilePoolRuntime struct {
	reconciled chan<- struct{}
}

func (runtime signallingReconcilePoolRuntime) RequestWork(
	context.Context,
	core.RunnerPool,
) (runtimeengine.ProcessResult, error) {
	return runtimeengine.ProcessResult{}, nil
}

func (runtime signallingReconcilePoolRuntime) Reconcile(context.Context, core.RunnerPool) error {
	close(runtime.reconciled)
	return nil
}

func (signallingReconcilePoolRuntime) RecoverForeign(context.Context, core.RunnerPool) error {
	return nil
}
