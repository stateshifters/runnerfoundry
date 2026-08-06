package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestUnifiedGenerationCyclesImmediatelyAndOnInjectedTicks(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 2)
	engine := &cycleEngineFake{cycles: cycles}
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newUnifiedGeneration(engine, ticker, nil, nil, nil)

	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForCycle(t, cycles)
	ticker.ticks <- time.Time{}
	waitForCycle(t, cycles)
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
	if !ticker.stopped || engine.closes != 1 {
		t.Fatalf("ticker stopped=%t engine closes=%d", ticker.stopped, engine.closes)
	}
}

func TestUnifiedGenerationDoesNotConsumeTicksBeforeActivation(t *testing.T) {
	t.Parallel()

	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	ticker.ticks <- time.Time{}
	observedPendingTicks := make(chan int, 2)
	generation := newUnifiedGeneration(
		&activationObservationEngine{ticker: ticker, observedPendingTicks: observedPendingTicks},
		ticker,
		nil,
		nil,
		nil,
	)
	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if err := generation.Activate(); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	if pendingTicks := <-observedPendingTicks; pendingTicks != 1 {
		t.Fatalf("pending ticks at activation cycle = %d, want pre-activation tick untouched", pendingTicks)
	}
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
}

func TestUnifiedGenerationCanStopWhileWaitingForActivation(t *testing.T) {
	t.Parallel()

	generation := newUnifiedGeneration(
		&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("stop inactive generation: %v", err)
	}
}

func TestUnifiedGenerationReportsActivationBoundaryFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("activation failed")
	engine := newGatedCycleEngine(&cycleEngineFake{}, leadershipFake{activateErr: wantErr, events: &[]string{}})
	generation := newUnifiedGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil)
	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}
	if err := generation.Activate(); !errors.Is(err, wantErr) {
		t.Fatalf("activation error = %v", err)
	}
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("stop inactive generation: %v", err)
	}
}

func TestUnifiedGenerationReportsCycleFailuresAndKeepsRunning(t *testing.T) {
	t.Parallel()

	cycleErr := errors.New("cycle failed")
	cycles := make(chan struct{}, 2)
	reports := make(chan error, 2)
	engine := &cycleEngineFake{cycles: cycles, cycleErr: cycleErr}
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newUnifiedGeneration(engine, ticker, func(err error) { reports <- err }, nil, nil)

	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForCycle(t, cycles)
	if err := <-reports; !errors.Is(err, cycleErr) {
		t.Fatalf("reported error = %v", err)
	}
	ticker.ticks <- time.Time{}
	waitForCycle(t, cycles)
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
}

func TestUnifiedGenerationClosesOwnedResourcesInReverseOrder(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 1)
	events := make([]string, 0)
	engine := &cycleEngineFake{cycles: cycles, closeEvents: &events}
	generation := newUnifiedGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, []runtimeCloser{
		closerFake{name: "state", events: &events}, closerFake{name: "provider", events: &events},
	}, nil)

	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForCycle(t, cycles)
	if err := generation.Stop(context.Background()); err != nil {
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
	generation := newUnifiedGeneration(
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
	assertNoTestError(t, replacement.Cycle(context.Background()), "cycle overlapping replacement")
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
	generation := newUnifiedGeneration(
		&cycleEngineFake{cycles: cycles}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil,
		nil,
	)
	assertTestError(t, generation.Activate(), "activate before start")
	assertNoTestError(t, generation.Start(context.Background()), "start generation")
	assertNoTestError(t, generation.Activate(), "activate generation")
	assertTestError(t, generation.Activate(), "activate twice")
	waitForCycle(t, cycles)
	assertTestError(t, generation.Start(context.Background()), "start twice")
	assertNoTestError(t, generation.Stop(context.Background()), "stop generation")
	assertTestError(t, generation.Start(context.Background()), "restart closed generation")
	assertTestError(t, generation.Activate(), "activate closed generation")
	assertNoTestError(t, generation.Stop(context.Background()), "stop generation twice")
}

func TestUnifiedGenerationStopsBeforeStartAndGatedLeadershipErrors(t *testing.T) {
	t.Parallel()

	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation := newUnifiedGeneration(&cycleEngineFake{cycles: make(chan struct{})}, ticker, nil, nil, nil)
	if err := generation.Stop(context.Background()); err != nil || !ticker.stopped {
		t.Fatalf("stop before start = %v, ticker stopped=%t", err, ticker.stopped)
	}
	if err := generation.Start(context.Background()); err == nil {
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

func TestUnifiedGenerationReportsCanceledWaitAndCloseFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("close failed")
	block := make(chan struct{})
	cycles := make(chan struct{}, 1)
	engine := &cycleEngineFake{cycles: cycles, block: block, closeErr: wantErr}
	generation := newUnifiedGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil,
		[]runtimeCloser{closerErrorFake{events: &[]string{}, err: wantErr}}, nil)
	if err := generation.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := generation.Activate(); err != nil {
		t.Fatal(err)
	}
	waitForCycle(t, cycles)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := generation.Stop(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
	}
	close(block)
	generation = newUnifiedGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil,
		[]runtimeCloser{closerErrorFake{events: &[]string{}, err: wantErr}}, nil)
	if err := generation.Stop(context.Background()); !errors.Is(err, wantErr) {
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

type activationObservationEngine struct {
	ticker               *manualCycleTicker
	observedPendingTicks chan<- int
}

func (engine *activationObservationEngine) Cycle(context.Context) error {
	engine.observedPendingTicks <- len(engine.ticker.ticks)
	return nil
}

func (*activationObservationEngine) Close(context.Context) error { return nil }

func (fake *cycleEngineFake) Cycle(context.Context) error {
	fake.cycles <- struct{}{}
	if fake.block != nil {
		<-fake.block
	}
	return fake.cycleErr
}
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
