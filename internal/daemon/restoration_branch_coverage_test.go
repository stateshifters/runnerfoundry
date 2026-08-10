package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestQueuedJobProcessorStopCoversIdleAndDeadlinePaths(t *testing.T) {
	processor := newQueuedJobProcessor(queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		return runtimeengine.ProcessResult{}, nil
	}}, newQueuedJobEligibilityGate(), nil)
	if err := processor.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() before start error = %v", err)
	}

	requestStarted := make(chan struct{})
	release := make(chan struct{})
	processor = newQueuedJobProcessor(queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		close(requestStarted)
		<-release
		return runtimeengine.ProcessResult{}, nil
	}}, newQueuedJobEligibilityGate(), nil)
	markQueuedJobPoolsEligible(processor.gate, "linux")
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	processor.Wake("linux")
	<-requestStarted
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := processor.Stop(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("deadline Stop() error = %v", err)
	}
	close(release)
	stopQueuedJobProcessor(t, processor)
}

func TestUnifiedEngineRequestWorkReportsPoolErrorsAndMissingPool(t *testing.T) {
	wantErr := errors.New("request failed")
	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	engine.pools[0].runtime = poolRuntimeFake{events: &events, requestErr: wantErr}
	if _, err := engine.RequestWork(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("RequestWork() error = %v, want %v", err, wantErr)
	}
	if _, err := engine.RequestWork(context.Background(), "missing"); err == nil {
		t.Fatal("missing Runner Pool unexpectedly succeeded")
	}
}

func TestUnifiedGenerationReportsQueuedJobProcessorStartFailure(t *testing.T) {
	requester := queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		return runtimeengine.ProcessResult{}, nil
	}}
	processor := newQueuedJobProcessor(requester, newQueuedJobEligibilityGate(), nil)
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("pre-start processor: %v", err)
	}
	cycles := make(chan struct{}, 1)
	generation := newMaintenanceGeneration(
		&cycleEngineFake{cycles: cycles}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	generation.processor = processor
	mustPrepareGeneration(t, generation)
	if err := generation.Activate(context.Background()); err == nil {
		t.Fatal("generation accepted an already-started processor")
	}
	stopQueuedJobProcessor(t, processor)
}

func TestUnifiedGenerationReportsQueuedJobProcessorStopFailure(t *testing.T) {
	requestStarted := make(chan struct{})
	release := make(chan struct{})
	processor := newQueuedJobProcessor(queuedJobRequesterFake{request: func(
		context.Context,
		core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		close(requestStarted)
		<-release
		return runtimeengine.ProcessResult{}, nil
	}}, newQueuedJobEligibilityGate(), nil)
	markQueuedJobPoolsEligible(processor.gate, "linux")
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	processor.Wake("linux")
	<-requestStarted
	done := make(chan struct{})
	close(done)
	generation := &maintenanceGeneration{processor: processor}
	if err := generation.stopActiveRuntime(newSecondDoneCanceledContext(), func() {}, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopActiveRuntime() error = %v, want cancellation", err)
	}
	close(release)
	stopQueuedJobProcessor(t, processor)
}

type secondDoneCanceledContext struct {
	mutex sync.Mutex
	calls int
	never chan struct{}
	done  chan struct{}
}

func newSecondDoneCanceledContext() *secondDoneCanceledContext {
	done := make(chan struct{})
	close(done)
	return &secondDoneCanceledContext{never: make(chan struct{}), done: done}
}

func (*secondDoneCanceledContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *secondDoneCanceledContext) Done() <-chan struct{} {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.calls++
	if ctx.calls == 1 {
		return ctx.never
	}
	return ctx.done
}

func (*secondDoneCanceledContext) Err() error    { return context.Canceled }
func (*secondDoneCanceledContext) Value(any) any { return nil }

func TestGitHubSourceRuntimeCoversStableAndRejectedLeadershipOpen(t *testing.T) {
	wantErr := errors.New("snapshot failed")
	manager := &githubManagerFake{snapshotErr: wantErr}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	if err := source.OpenLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("OpenLeader() error = %v, want %v", err, wantErr)
	}
	manager.setSnapshotError(nil)
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("stable OpenLeader() error = %v", err)
	}
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeIgnoresDuplicateDiscoveryAndSessionScheduling(t *testing.T) {
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux"}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{{RunnerPool: "linux", Repository: "repo"}}}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	term := &githubLeadershipTerm{discoveryActive: true}
	source.startDiscovery(context.Background(), nil)
	source.startDiscovery(context.Background(), term)

	source.desired[registrationKey(record)] = record
	source.opening[registrationKey(record)] = record
	source.scheduleRequiredRegistrationSessions(context.Background(), term)
	if len(term.operations) != 0 {
		t.Fatalf("duplicate schedule started %d operations", len(term.operations))
	}
}
