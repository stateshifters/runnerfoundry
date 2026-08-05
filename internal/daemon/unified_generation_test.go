package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestUnifiedGenerationCyclesImmediatelyAndOnInjectedTicks(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 2)
	engine := &cycleEngineFake{cycles: cycles}
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newUnifiedGeneration(engine, ticker, nil, nil)

	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
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

func TestUnifiedGenerationReportsCycleFailuresAndKeepsRunning(t *testing.T) {
	t.Parallel()

	cycleErr := errors.New("cycle failed")
	cycles := make(chan struct{}, 2)
	reports := make(chan error, 2)
	engine := &cycleEngineFake{cycles: cycles, cycleErr: cycleErr}
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newUnifiedGeneration(engine, ticker, func(err error) { reports <- err }, nil)

	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
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
	})

	if err := generation.Start(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	waitForCycle(t, cycles)
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
	if want := []string{"engine", "provider", "state"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("close events = %#v, want %#v", events, want)
	}
}

func TestGatedCycleEnginePreventsReloadGenerationOverlap(t *testing.T) {
	t.Parallel()

	gate := &localLeadershipGate{}
	activeCycles := make(chan struct{}, 1)
	replacementCycles := make(chan struct{}, 1)
	active := newGatedCycleEngine(&cycleEngineFake{cycles: activeCycles}, gate.NewLeadership())
	replacement := newGatedCycleEngine(&cycleEngineFake{cycles: replacementCycles}, gate.NewLeadership())

	if err := active.Cycle(context.Background()); err != nil {
		t.Fatalf("cycle active generation: %v", err)
	}
	waitForCycle(t, activeCycles)
	if err := replacement.Cycle(context.Background()); err != nil {
		t.Fatalf("cycle overlapping replacement: %v", err)
	}
	if len(replacementCycles) != 0 {
		t.Fatal("replacement generation cycled before active generation closed")
	}
	if err := active.Close(context.Background()); err != nil {
		t.Fatalf("close active generation: %v", err)
	}
	if err := replacement.Cycle(context.Background()); err != nil {
		t.Fatalf("cycle replacement after handoff: %v", err)
	}
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
	)
	if err := generation.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForCycle(t, cycles)
	if err := generation.Start(context.Background()); err == nil {
		t.Fatal("generation started twice")
	}
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := generation.Start(context.Background()); err == nil {
		t.Fatal("closed generation restarted")
	}
	if err := generation.Stop(context.Background()); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestUnifiedGenerationStopsBeforeStartAndGatedLeadershipErrors(t *testing.T) {
	t.Parallel()

	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation := newUnifiedGeneration(&cycleEngineFake{cycles: make(chan struct{})}, ticker, nil, nil)
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
	gated = newGatedCycleEngine(&cycleEngineFake{cycles: make(chan struct{}, 1), cycleErr: wantErr},
		(&localLeadershipGate{}).NewLeadership())
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
		[]runtimeCloser{closerErrorFake{events: &[]string{}, err: wantErr}})
	if err := generation.Start(context.Background()); err != nil {
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
		[]runtimeCloser{closerErrorFake{events: &[]string{}, err: wantErr}})
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
