package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func mustPrepareGeneration(t *testing.T, generation *maintenanceGeneration) {
	t.Helper()
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
}

func mustActivateGeneration(t *testing.T, generation *maintenanceGeneration) {
	t.Helper()
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
}

func mustDeactivateGeneration(t *testing.T, generation *maintenanceGeneration) {
	t.Helper()
	if err := generation.Deactivate(context.Background()); err != nil {
		t.Fatalf("deactivate generation: %v", err)
	}
}

func mustCloseGeneration(t *testing.T, generation *maintenanceGeneration) {
	t.Helper()
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("close generation: %v", err)
	}
}

func TestUnifiedGenerationCyclesImmediatelyAndOnInjectedTicks(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 2)
	engine := &cycleEngineFake{cycles: cycles}
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newMaintenanceGeneration(engine, ticker, nil, nil, nil)

	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForCycle(t, cycles)
	ticker.ticks <- time.Time{}
	waitForCycle(t, cycles)
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
	if !ticker.stopped || engine.closes != 1 {
		t.Fatalf("ticker stopped=%t engine closes=%d", ticker.stopped, engine.closes)
	}
}

func TestUnifiedGenerationActivatesBeforeFirstMaintenanceCycleCompletes(t *testing.T) {
	t.Parallel()

	generation, cycles, releaseCycle := newBlockedMaintenanceGeneration(t)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}

	activationResult := make(chan error, 1)
	go func() { activationResult <- generation.Activate(context.Background()) }()
	waitForCycle(t, cycles)
	select {
	case err := <-activationResult:
		if err != nil {
			t.Fatalf("activate generation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("activation waited for the first maintenance cycle")
	}

	releaseCycle()
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("close generation: %v", err)
	}
}

func TestUnifiedGenerationStartsAndJoinsQueuedJobProcessorWithinActivation(t *testing.T) {
	requestStarted := make(chan struct{})
	requestStopped := make(chan struct{})
	requester := queuedJobRequesterFake{request: func(
		ctx context.Context,
		_ core.RunnerPool,
	) (runtimeengine.ProcessResult, error) {
		close(requestStarted)
		<-ctx.Done()
		close(requestStopped)
		return runtimeengine.ProcessResult{}, ctx.Err()
	}}
	gate := newQueuedJobEligibilityGate()
	gate.Maintain(func(maintenance *queuedJobMaintenance) {
		maintenance.MarkEligible("linux")
	})
	processor := newQueuedJobProcessor(requester, gate, nil)
	processor.Wake("linux")
	engine := &processorLifecycleEngineFake{processorStopped: requestStopped}
	generation := newMaintenanceGeneration(
		engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	generation.processor = processor
	mustPrepareGeneration(t, generation)

	mustActivateGeneration(t, generation)
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("queued-job processor did not start with the generation")
	}
	mustDeactivateGeneration(t, generation)
	mustCloseGeneration(t, generation)
}

func TestUnifiedGenerationSerializesImmediateAndScheduledMaintenance(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 2)
	cycleResults := make(chan error, 2)
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newMaintenanceGeneration(
		&controlledCycleEngineFake{cycles: cycles, results: cycleResults},
		ticker,
		nil,
		nil,
		nil,
	)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForCycle(t, cycles)
	ticker.ticks <- time.Time{}
	select {
	case <-cycles:
		t.Fatal("scheduled maintenance overlapped the immediate cycle")
	default:
	}
	cycleResults <- nil
	waitForCycle(t, cycles)
	cycleResults <- nil
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("close generation: %v", err)
	}
}

func TestUnifiedGenerationOwnsQueueNotificationLifecycle(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	notifications := &queueNotificationRuntimeFake{started: started, stopped: stopped}
	generation := newMaintenanceGeneration(
		&cycleEngineFake{cycles: make(chan struct{}, 1)},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	generation.notifications = notifications
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	<-started
	mustDeactivateGeneration(t, generation)
	<-stopped
	mustCloseGeneration(t, generation)
}

func TestUnifiedGenerationReportsQueueNotificationStopFailure(t *testing.T) {
	wantErr := errors.New("notification stop failed")
	notifications := &queueNotificationRuntimeFake{
		started: make(chan struct{}), stopped: make(chan struct{}), stopErr: wantErr,
	}
	generation := newMaintenanceGeneration(
		&cycleEngineFake{cycles: make(chan struct{}, 1)},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	generation.notifications = notifications
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	if err := generation.Deactivate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("deactivation error = %v, want %v", err, wantErr)
	}
}

func TestUnifiedGenerationDoesNotConsumeTicksBeforeActivation(t *testing.T) {
	t.Parallel()

	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	ticker.ticks <- time.Time{}
	observedPendingTicks := make(chan int, 2)
	generation := newMaintenanceGeneration(
		&activationObservationEngine{ticker: ticker, observedPendingTicks: observedPendingTicks},
		ticker,
		nil,
		nil,
		nil,
	)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	if pendingTicks := <-observedPendingTicks; pendingTicks != 1 {
		t.Fatalf("pending ticks at activation cycle = %d, want pre-activation tick untouched", pendingTicks)
	}
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
}

func TestUnifiedGenerationCanStopWhileWaitingForActivation(t *testing.T) {
	t.Parallel()

	generation := newMaintenanceGeneration(
		&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("stop inactive generation: %v", err)
	}
}

func TestUnifiedGenerationReportsActivationBoundaryFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("activation failed")
	engine := newGatedCycleEngine(&cycleEngineFake{}, leadershipFake{activateErr: wantErr, events: &[]string{}})
	generation := newMaintenanceGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if err := generation.Activate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("activation error = %v", err)
	}
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("stop inactive generation: %v", err)
	}
}

func TestUnifiedGenerationClosesOwnedResourcesInReverseOrder(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 1)
	events := make([]string, 0)
	engine := &cycleEngineFake{cycles: cycles, closeEvents: &events}
	generation := newMaintenanceGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, []runtimeCloser{
		closerFake{name: "state", events: &events}, closerFake{name: "provider", events: &events},
	}, nil)

	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForCycle(t, cycles)
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
	if want := []string{"engine", "provider", "state"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("close events = %#v, want %#v", events, want)
	}
}

func TestUnifiedGenerationSelectsFinalizersByRunnerPool(t *testing.T) {
	t.Parallel()

	linux := &finalizerFake{runnerPool: "linux"}
	windows := &finalizerFake{runnerPool: "windows"}
	generation := newMaintenanceGeneration(
		&cycleEngineFake{},
		&manualCycleTicker{ticks: make(chan time.Time)},
		nil,
		nil,
		[]runtimeFinalizer{linux, windows},
	)
	selected := generation.Finalizers([]core.RunnerPool{"windows"})
	if len(selected) != 1 || selected[0] != windows {
		t.Fatalf("selected finalizers = %#v, want windows", selected)
	}
}

func TestGatedCycleEnginePreventsReloadGenerationOverlap(t *testing.T) {
	t.Parallel()

	gate := &localLeadershipGate{}
	activeCycles := make(chan struct{}, 1)
	replacementCycles := make(chan struct{}, 1)
	active := newGatedCycleEngine(&cycleEngineFake{cycles: activeCycles}, gate.NewLeadership())
	replacement := newGatedCycleEngine(&cycleEngineFake{cycles: replacementCycles}, gate.NewLeadership())

	assertNoTestError(t, active.Activate(), "activate initial generation")
	assertNoTestError(t, active.Cycle(context.Background()), "cycle active generation")
	assertTestError(t, replacement.Activate(), "activate overlapping replacement")
	waitForCycle(t, activeCycles)
	assertTestError(t, replacement.Cycle(context.Background()), "cycle overlapping replacement")
	if len(replacementCycles) != 0 {
		t.Fatal("replacement generation cycled before active generation closed")
	}
	assertNoTestError(t, active.Close(context.Background()), "close active generation")
	assertNoTestError(t, replacement.Activate(), "activate replacement generation")
	assertNoTestError(t, replacement.Cycle(context.Background()), "cycle replacement after handoff")
	waitForCycle(t, replacementCycles)
}

func TestCloseRuntimeResourcesReversesBuildOwnershipAndJoinsFailures(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first close failed")
	secondErr := errors.New("second close failed")
	events := make([]string, 0)
	err := closeRuntimeResources(context.Background(), []runtimeCloser{
		closerErrorFake{name: "first", events: &events, err: firstErr},
		closerErrorFake{name: "second", events: &events, err: secondErr},
	})
	if want := []string{"second", "first"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("close events = %#v, want %#v", events, want)
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("close error = %v, want both failures", err)
	}
}

func TestUnifiedGenerationRejectsInvalidLifecycleCalls(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 1)
	generation := newMaintenanceGeneration(
		&cycleEngineFake{cycles: cycles}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil,
		nil,
	)
	assertTestError(t, generation.Activate(context.Background()), "activate before start")
	assertNoTestError(t, generation.Prepare(context.Background()), "prepare generation")
	assertNoTestError(t, generation.Activate(context.Background()), "activate generation")
	assertTestError(t, generation.Activate(context.Background()), "activate twice")
	waitForCycle(t, cycles)
	assertTestError(t, generation.Prepare(context.Background()), "prepare twice")
	assertNoTestError(t, generation.Close(context.Background()), "close generation")
	assertTestError(t, generation.Prepare(context.Background()), "prepare closed generation")
	assertTestError(t, generation.Activate(context.Background()), "activate closed generation")
	assertNoTestError(t, generation.Close(context.Background()), "close generation twice")
}

func TestUnifiedGenerationStopsBeforeStartAndGatedLeadershipErrors(t *testing.T) {
	t.Parallel()

	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation := newMaintenanceGeneration(&cycleEngineFake{cycles: make(chan struct{})}, ticker, nil, nil, nil)
	if err := generation.Close(context.Background()); err != nil || !ticker.stopped {
		t.Fatalf("stop before start = %v, ticker stopped=%t", err, ticker.stopped)
	}
	if err := generation.Prepare(context.Background()); err == nil {
		t.Fatal("generation started after close-before-start")
	}
	wantErr := errors.New("leadership failed")
	gated := newGatedCycleEngine(&cycleEngineFake{cycles: make(chan struct{}, 1)}, leadershipFake{err: wantErr, events: &[]string{}})
	if err := gated.Cycle(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("gated cycle error = %v", err)
	}
	gated = newGatedCycleEngine(&cycleEngineFake{}, leadershipFake{activateErr: wantErr, events: &[]string{}})
	if err := gated.Activate(); !errors.Is(err, wantErr) {
		t.Fatalf("gated activation error = %v", err)
	}
	gated = newGatedCycleEngine(&cycleEngineFake{cycles: make(chan struct{}, 1), cycleErr: wantErr},
		(&localLeadershipGate{}).NewLeadership())
	if err := gated.Activate(); err != nil {
		t.Fatalf("activate gated engine: %v", err)
	}
	if err := gated.Cycle(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("active cycle error = %v", err)
	}
}

func TestUnifiedGenerationReportsCanceledWait(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	cycles := make(chan struct{}, 1)
	cycleBlocks := make(chan (<-chan struct{}), 2)
	cycleBlocks <- nil
	cycleBlocks <- block
	engine := &sequencedBlockingCycleEngineFake{cycles: cycles, blocks: cycleBlocks}
	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation := newMaintenanceGeneration(engine, ticker, nil, nil, nil)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForCycle(t, cycles)
	ticker.ticks <- time.Time{}
	waitForCycle(t, cycles)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := generation.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
	}
	close(block)
}

func TestUnifiedGenerationReportsCloseFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	generation := newMaintenanceGeneration(&cycleEngineFake{closeErr: wantErr},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil,
		[]runtimeCloser{closerErrorFake{events: &[]string{}, err: wantErr}}, nil)
	if err := generation.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v", err)
	}
}

type cycleEngineFake struct {
	cycles      chan<- struct{}
	cycleErr    error
	closes      int
	closeEvents *[]string
	block       <-chan struct{}
	closeErr    error
}

type controlledCycleEngineFake struct {
	cycles  chan<- struct{}
	results <-chan error
}

type processorLifecycleEngineFake struct {
	processorStopped <-chan struct{}
}

type queueNotificationRuntimeFake struct {
	started chan<- struct{}
	stopped chan<- struct{}
	stopErr error
}

func (runtime *queueNotificationRuntimeFake) Start(context.Context) { close(runtime.started) }
func (runtime *queueNotificationRuntimeFake) Stop(context.Context) error {
	close(runtime.stopped)
	return runtime.stopErr
}

func (*processorLifecycleEngineFake) Activate() error { return nil }
func (*processorLifecycleEngineFake) Cycle(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
func (fake *processorLifecycleEngineFake) Deactivate(context.Context) error {
	select {
	case <-fake.processorStopped:
		return nil
	default:
		return errors.New("engine deactivated before queued-job processor stopped")
	}
}
func (*processorLifecycleEngineFake) Close(context.Context) error { return nil }

func (*controlledCycleEngineFake) Activate() error { return nil }
func (*controlledCycleEngineFake) Deactivate(context.Context) error {
	return nil
}
func (fake *controlledCycleEngineFake) Cycle(ctx context.Context) error {
	fake.cycles <- struct{}{}
	select {
	case err := <-fake.results:
		return err
	case <-ctx.Done():
		return fmt.Errorf("wait for controlled cycle result: %w", ctx.Err())
	}
}
func (*controlledCycleEngineFake) Close(context.Context) error { return nil }

type sequencedBlockingCycleEngineFake struct {
	cycles chan<- struct{}
	blocks <-chan (<-chan struct{})
}

func (*sequencedBlockingCycleEngineFake) Activate() error { return nil }
func (*sequencedBlockingCycleEngineFake) Deactivate(context.Context) error {
	return nil
}
func (fake *sequencedBlockingCycleEngineFake) Cycle(context.Context) error {
	fake.cycles <- struct{}{}
	if block := <-fake.blocks; block != nil {
		<-block
	}
	return nil
}
func (*sequencedBlockingCycleEngineFake) Close(context.Context) error { return nil }

func newBlockedMaintenanceGeneration(t *testing.T) (*maintenanceGeneration, <-chan struct{}, func()) {
	t.Helper()
	cycles := make(chan struct{}, 1)
	releaseCycle := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCycle) }) }
	t.Cleanup(release)
	generation := newMaintenanceGeneration(
		&cycleEngineFake{cycles: cycles, block: releaseCycle},
		&manualCycleTicker{ticks: make(chan time.Time)},
		nil,
		nil,
		nil,
	)
	return generation, cycles, release
}

type activationObservationEngine struct {
	ticker               *manualCycleTicker
	observedPendingTicks chan<- int
}

func (engine *activationObservationEngine) Cycle(context.Context) error {
	engine.observedPendingTicks <- len(engine.ticker.ticks)
	return nil
}

func (*activationObservationEngine) Activate() error                  { return nil }
func (*activationObservationEngine) Deactivate(context.Context) error { return nil }

func (*activationObservationEngine) Close(context.Context) error { return nil }

func (fake *cycleEngineFake) Cycle(context.Context) error {
	fake.cycles <- struct{}{}
	if fake.block != nil {
		<-fake.block
	}
	return fake.cycleErr
}
func (*cycleEngineFake) Activate() error                  { return nil }
func (*cycleEngineFake) Deactivate(context.Context) error { return nil }
func (fake *cycleEngineFake) Close(context.Context) error {
	fake.closes++
	if fake.closeEvents != nil {
		*fake.closeEvents = append(*fake.closeEvents, "engine")
	}
	return fake.closeErr
}

type manualCycleTicker struct {
	ticks   chan time.Time
	stopped bool
}

func (ticker *manualCycleTicker) Ticks() <-chan time.Time { return ticker.ticks }
func (ticker *manualCycleTicker) Stop()                   { ticker.stopped = true }

type closerFake struct {
	name   string
	events *[]string
}

type closerErrorFake struct {
	name   string
	events *[]string
	err    error
}

type finalizerFake struct {
	runnerPool core.RunnerPool
	err        error
	calls      int
}

func (fake *finalizerFake) RunnerPool() core.RunnerPool { return fake.runnerPool }
func (fake *finalizerFake) Terminate(context.Context) (int, error) {
	fake.calls++
	return 0, fake.err
}

func (closer closerErrorFake) Close(context.Context) error {
	*closer.events = append(*closer.events, closer.name)
	return closer.err
}

func (closer closerFake) Close(context.Context) error {
	*closer.events = append(*closer.events, closer.name)
	return nil
}

func waitForCycle(t *testing.T, cycles <-chan struct{}) {
	t.Helper()
	<-cycles
}
