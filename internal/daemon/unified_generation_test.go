package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
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

func TestUnifiedGenerationMaintainsMembershipImmediatelyAndOnInjectedTicks(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 2)
	engine := &maintenanceEngineFake{cycles: cycles}
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newTestMaintenanceGeneration(engine, ticker, nil, nil, nil)

	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForMaintenance(t, cycles)
	ticker.ticks <- time.Time{}
	waitForMaintenance(t, cycles)
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("stop generation: %v", err)
	}
	if !ticker.stopped || engine.closes != 1 {
		t.Fatalf("ticker stopped=%t engine closes=%d", ticker.stopped, engine.closes)
	}
}

func TestUnifiedGenerationActivatesBeforeFirstMaintenanceCycleCompletes(t *testing.T) {
	t.Parallel()

	generation, cycles, releaseMaintenance := newBlockedMaintenanceGeneration(t)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare generation: %v", err)
	}

	activationResult := make(chan error, 1)
	go func() { activationResult <- generation.Activate(context.Background()) }()
	waitForMaintenance(t, cycles)
	select {
	case err := <-activationResult:
		if err != nil {
			t.Fatalf("activate generation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("activation waited for the first maintenance cycle")
	}

	releaseMaintenance()
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
	markQueuedJobPoolsEligible(t, gate, "linux")
	processor := newQueuedJobProcessorForPools(requester, gate, nil, []core.RunnerPool{"linux"})
	processor.Wake("linux")
	engine := &processorLifecycleEngineFake{processorStopped: requestStopped}
	generation := newTestMaintenanceGeneration(
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

func TestUnifiedGenerationSerializesImmediateAndScheduledLaneMaintenance(t *testing.T) {
	t.Parallel()

	cycles := make(chan struct{}, 2)
	maintenanceResults := make(chan error, 2)
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newTestMaintenanceGeneration(
		&controlledMaintenanceEngineFake{cycles: cycles, results: maintenanceResults},
		ticker,
		nil,
		nil,
		nil,
	)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForMaintenance(t, cycles)
	ticker.ticks <- time.Time{}
	select {
	case <-cycles:
		t.Fatal("scheduled maintenance overlapped the immediate cycle")
	default:
	}
	maintenanceResults <- nil
	waitForMaintenance(t, cycles)
	maintenanceResults <- nil
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("close generation: %v", err)
	}
}

func TestUnifiedGenerationOwnsQueueNotificationLifecycle(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	notifications := &queueNotificationRuntimeFake{started: started, stopped: stopped}
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: make(chan struct{}, 1)},
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

func TestUnifiedGenerationRejectsQueueNotificationStartupFailureAndStopsProcessor(t *testing.T) {
	wantErr := errors.New("notification startup failed")
	notifications := &queueNotificationRuntimeFake{
		started: make(chan struct{}), stopped: make(chan struct{}), startErr: wantErr,
	}
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: make(chan struct{}, 1)},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	processor := newQueuedJobProcessorForPools(
		queuedJobRequesterFake{}, newQueuedJobEligibilityGate(), nil, []core.RunnerPool{"linux"},
	)
	generation.processor = processor
	generation.notifications = notifications
	mustPrepareGeneration(t, generation)
	if err := generation.Activate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("activation error = %v, want %v", err, wantErr)
	}
	assertQueuedJobProcessorCanRestart(t, processor)
	mustCloseGeneration(t, generation)
}

func TestUnifiedGenerationCancelsBlockedQueueNotificationStartupAndStopsProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	listener, connection := newCancelingQueueNotificationListener(cancel)
	processor := newQueuedJobProcessorForPools(
		queuedJobRequesterFake{}, newQueuedJobEligibilityGate(), nil, []core.RunnerPool{"linux"},
	)
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: make(chan struct{}, 1)},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	generation.processor = processor
	generation.notifications = listener
	mustPrepareGeneration(t, generation)

	if err := generation.Activate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("activation error = %v, want context cancellation", err)
	}
	if connection.discardCalls != 1 {
		t.Fatalf("canceled notification startup discards = %d, want 1", connection.discardCalls)
	}
	assertQueuedJobProcessorCanRestart(t, processor)
	mustCloseGeneration(t, generation)
}

func newCancelingQueueNotificationListener(
	cancel context.CancelFunc,
) (*postgreSQLQueueNotificationListener, *queueNotificationConnectionFake) {
	connection := newQueueNotificationConnectionFake()
	protocol := connection.protocol()
	protocol.listen = func(ctx context.Context) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire: func(context.Context) (queueNotificationConnection, error) {
			return protocol, nil
		},
	})
	return listener, connection
}

func assertQueuedJobProcessorCanRestart(t *testing.T, processor *queuedJobProcessor) {
	t.Helper()
	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("queued-job processor was not stopped after notification startup failure: %v", err)
	}
	assertNoTestError(t, processor.Stop(context.Background()), "stop restarted queued-job processor")
}

func TestUnifiedGenerationRunsMaintenanceWhileQueueNotificationPingIsBlocked(t *testing.T) {
	connection := newQueueNotificationConnectionFake()
	pingStarted := make(chan struct{})
	protocol := connection.protocol()
	protocol.ping = func(ctx context.Context) error {
		close(pingStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return protocol, nil },
	})
	listener.keepaliveInterval = time.Millisecond
	cycles := make(chan struct{}, 1)
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: cycles},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
	)
	generation.notifications = listener

	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	<-pingStarted
	waitForMaintenance(t, cycles)
	mustCloseGeneration(t, generation)
}

func TestUnifiedGenerationReportsQueueNotificationStopFailure(t *testing.T) {
	wantErr := errors.New("notification stop failed")
	notifications := &queueNotificationRuntimeFake{
		started: make(chan struct{}), stopped: make(chan struct{}), stopErr: wantErr,
	}
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: make(chan struct{}, 1)},
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
	generation := newTestMaintenanceGeneration(
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

	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil,
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
	engine := newGatedMaintenanceEngine(&maintenanceEngineFake{}, leadershipFake{activateErr: wantErr, events: &[]string{}})
	generation := newTestMaintenanceGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil)
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
	engine := &maintenanceEngineFake{cycles: cycles, closeEvents: &events}
	generation := newTestMaintenanceGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, []runtimeCloser{
		closerFake{name: "state", events: &events}, closerFake{name: "provider", events: &events},
	}, nil)

	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatalf("start generation: %v", err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	waitForMaintenance(t, cycles)
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
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{},
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

func TestGatedMaintenanceEnginePreventsReloadGenerationLaneOverlap(t *testing.T) {
	t.Parallel()

	gate := &localLeadershipGate{}
	activeCycles := make(chan struct{}, 1)
	replacementCycles := make(chan struct{}, 1)
	active := newGatedMaintenanceEngine(&maintenanceEngineFake{cycles: activeCycles}, gate.NewLeadership())
	replacement := newGatedMaintenanceEngine(&maintenanceEngineFake{cycles: replacementCycles}, gate.NewLeadership())

	assertNoTestError(t, active.Activate(), "activate initial generation")
	assertNoTestError(t, active.MaintainMembership(context.Background()), "maintain active generation")
	assertTestError(t, replacement.Activate(), "activate overlapping replacement")
	waitForMaintenance(t, activeCycles)
	assertTestError(t, replacement.MaintainMembership(context.Background()), "maintain overlapping replacement")
	if len(replacementCycles) != 0 {
		t.Fatal("replacement generation ran maintenance before active generation closed")
	}
	assertNoTestError(t, active.Close(context.Background()), "close active generation")
	assertNoTestError(t, replacement.Activate(), "activate replacement generation")
	assertNoTestError(t, replacement.MaintainMembership(context.Background()), "maintain replacement after handoff")
	waitForMaintenance(t, replacementCycles)
}

func TestGatedMaintenanceEngineRunsIndependentMaintenanceLanes(t *testing.T) {
	cycles := make(chan struct{}, 1)
	gate := &localLeadershipGate{}
	engine := newGatedMaintenanceEngine(&maintenanceEngineFake{cycles: cycles}, gate.NewLeadership())
	assertNoTestError(t, engine.Activate(), "activate gated engine")
	assertNoTestError(t, engine.MaintainMembership(context.Background()), "maintain membership")
	waitForMaintenance(t, cycles)
	assertNoTestError(t, engine.MaintainSource(context.Background()), "maintain source")
	assertNoTestError(t, engine.MaintainExecution(context.Background()), "maintain execution")
	assertNoTestError(t, engine.Close(context.Background()), "close gated engine")
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
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: cycles}, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil,
		nil,
	)
	assertTestError(t, generation.Activate(context.Background()), "activate before start")
	assertNoTestError(t, generation.Prepare(context.Background()), "prepare generation")
	assertNoTestError(t, generation.Activate(context.Background()), "activate generation")
	assertTestError(t, generation.Activate(context.Background()), "activate twice")
	waitForMaintenance(t, cycles)
	assertTestError(t, generation.Prepare(context.Background()), "prepare twice")
	assertNoTestError(t, generation.Close(context.Background()), "close generation")
	assertTestError(t, generation.Prepare(context.Background()), "prepare closed generation")
	assertTestError(t, generation.Activate(context.Background()), "activate closed generation")
	assertNoTestError(t, generation.Close(context.Background()), "close generation twice")
}

func TestUnifiedGenerationStopsBeforeStartAndGatedLeadershipErrors(t *testing.T) {
	t.Parallel()

	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation := newTestMaintenanceGeneration(&maintenanceEngineFake{cycles: make(chan struct{})}, ticker, nil, nil, nil)
	if err := generation.Close(context.Background()); err != nil || !ticker.stopped {
		t.Fatalf("stop before start = %v, ticker stopped=%t", err, ticker.stopped)
	}
	if err := generation.Prepare(context.Background()); err == nil {
		t.Fatal("generation started after close-before-start")
	}
	wantErr := errors.New("leadership failed")
	gated := newGatedMaintenanceEngine(&maintenanceEngineFake{cycles: make(chan struct{}, 1)}, leadershipFake{err: wantErr, events: &[]string{}})
	if err := gated.MaintainMembership(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("gated maintenance error = %v", err)
	}
	gated = newGatedMaintenanceEngine(&maintenanceEngineFake{}, leadershipFake{activateErr: wantErr, events: &[]string{}})
	if err := gated.Activate(); !errors.Is(err, wantErr) {
		t.Fatalf("gated activation error = %v", err)
	}
	gated = newGatedMaintenanceEngine(&maintenanceEngineFake{cycles: make(chan struct{}, 1), maintenanceErr: wantErr},
		(&localLeadershipGate{}).NewLeadership())
	if err := gated.Activate(); err != nil {
		t.Fatalf("activate gated engine: %v", err)
	}
	if err := gated.MaintainMembership(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("active maintenance error = %v", err)
	}
}

func TestUnifiedGenerationReportsCanceledWait(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	cycles := make(chan struct{}, 1)
	maintenanceBlocks := make(chan (<-chan struct{}), 2)
	maintenanceBlocks <- nil
	maintenanceBlocks <- block
	engine := &sequencedBlockingMaintenanceEngineFake{cycles: cycles, blocks: maintenanceBlocks}
	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	generation := newTestMaintenanceGeneration(engine, ticker, nil, nil, nil)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForMaintenance(t, cycles)
	ticker.ticks <- time.Time{}
	waitForMaintenance(t, cycles)
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
	generation := newTestMaintenanceGeneration(&maintenanceEngineFake{closeErr: wantErr},
		&manualCycleTicker{ticks: make(chan time.Time)}, nil,
		[]runtimeCloser{closerErrorFake{events: &[]string{}, err: wantErr}}, nil)
	if err := generation.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v", err)
	}
}

type maintenanceEngineFake struct {
	cycles         chan<- struct{}
	maintenanceErr error
	closes         int
	closeEvents    *[]string
	block          <-chan struct{}
	closeErr       error
}

type controlledMaintenanceEngineFake struct {
	cycles  chan<- struct{}
	results <-chan error
}

type processorLifecycleEngineFake struct {
	processorStopped <-chan struct{}
}

type queueNotificationRuntimeFake struct {
	started  chan<- struct{}
	stopped  chan<- struct{}
	startErr error
	stopErr  error
}

func (runtime *queueNotificationRuntimeFake) Start(context.Context, context.Context) error {
	close(runtime.started)
	return runtime.startErr
}
func (runtime *queueNotificationRuntimeFake) Stop(context.Context) error {
	close(runtime.stopped)
	return runtime.stopErr
}

func (*processorLifecycleEngineFake) Activate() error { return nil }
func (*processorLifecycleEngineFake) MaintainMembership(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
func (*processorLifecycleEngineFake) MaintainSource(context.Context) error    { return nil }
func (*processorLifecycleEngineFake) MaintainExecution(context.Context) error { return nil }
func (fake *processorLifecycleEngineFake) Deactivate(context.Context) error {
	select {
	case <-fake.processorStopped:
		return nil
	default:
		return errors.New("engine deactivated before queued-job processor stopped")
	}
}
func (*processorLifecycleEngineFake) Close(context.Context) error { return nil }

func (*controlledMaintenanceEngineFake) Activate() error { return nil }
func (*controlledMaintenanceEngineFake) Deactivate(context.Context) error {
	return nil
}
func (fake *controlledMaintenanceEngineFake) MaintainMembership(ctx context.Context) error {
	fake.cycles <- struct{}{}
	select {
	case err := <-fake.results:
		return err
	case <-ctx.Done():
		return fmt.Errorf("wait for controlled maintenance result: %w", ctx.Err())
	}
}
func (*controlledMaintenanceEngineFake) MaintainSource(context.Context) error    { return nil }
func (*controlledMaintenanceEngineFake) MaintainExecution(context.Context) error { return nil }
func (*controlledMaintenanceEngineFake) Close(context.Context) error             { return nil }

type sequencedBlockingMaintenanceEngineFake struct {
	cycles chan<- struct{}
	blocks <-chan (<-chan struct{})
}

func (*sequencedBlockingMaintenanceEngineFake) Activate() error { return nil }
func (*sequencedBlockingMaintenanceEngineFake) Deactivate(context.Context) error {
	return nil
}
func (fake *sequencedBlockingMaintenanceEngineFake) MaintainMembership(context.Context) error {
	fake.cycles <- struct{}{}
	if block := <-fake.blocks; block != nil {
		<-block
	}
	return nil
}
func (*sequencedBlockingMaintenanceEngineFake) MaintainSource(context.Context) error    { return nil }
func (*sequencedBlockingMaintenanceEngineFake) MaintainExecution(context.Context) error { return nil }
func (*sequencedBlockingMaintenanceEngineFake) Close(context.Context) error             { return nil }

func newBlockedMaintenanceGeneration(t *testing.T) (*maintenanceGeneration, <-chan struct{}, func()) {
	t.Helper()
	cycles := make(chan struct{}, 1)
	releaseMaintenance := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseMaintenance) }) }
	t.Cleanup(release)
	generation := newTestMaintenanceGeneration(
		&maintenanceEngineFake{cycles: cycles, block: releaseMaintenance},
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

func (engine *activationObservationEngine) MaintainMembership(context.Context) error {
	engine.observedPendingTicks <- len(engine.ticker.ticks)
	return nil
}
func (*activationObservationEngine) MaintainSource(context.Context) error    { return nil }
func (*activationObservationEngine) MaintainExecution(context.Context) error { return nil }

func (*activationObservationEngine) Activate() error                  { return nil }
func (*activationObservationEngine) Deactivate(context.Context) error { return nil }

func (*activationObservationEngine) Close(context.Context) error { return nil }

func (fake *maintenanceEngineFake) MaintainMembership(context.Context) error {
	fake.cycles <- struct{}{}
	if fake.block != nil {
		<-fake.block
	}
	return fake.maintenanceErr
}

func (*maintenanceEngineFake) MaintainSource(context.Context) error    { return nil }
func (*maintenanceEngineFake) MaintainExecution(context.Context) error { return nil }
func (*maintenanceEngineFake) Activate() error                         { return nil }
func (*maintenanceEngineFake) Deactivate(context.Context) error        { return nil }
func (fake *maintenanceEngineFake) Close(context.Context) error {
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

type maintenanceTestEngine interface {
	generationLifecycleEngine
	MaintainMembership(ctx context.Context) error
}

type membershipOnlyTestRuntime struct {
	engine maintenanceTestEngine
}

func (runtime membershipOnlyTestRuntime) MaintainMembership(ctx context.Context) error {
	if err := runtime.engine.MaintainMembership(ctx); err != nil {
		return fmt.Errorf("maintain membership: %w", err)
	}
	return nil
}

func (membershipOnlyTestRuntime) MaintainSource(ctx context.Context) error {
	<-ctx.Done()
	return fmt.Errorf("wait for source maintenance cancellation: %w", ctx.Err())
}

func (membershipOnlyTestRuntime) MaintainExecution(ctx context.Context) error {
	<-ctx.Done()
	return fmt.Errorf("wait for execution maintenance cancellation: %w", ctx.Err())
}

func newTestMaintenanceGeneration(
	engine maintenanceTestEngine,
	membershipTicker cycleTicker,
	report func(error),
	closers []runtimeCloser,
	finalizers []runtimeFinalizer,
) *maintenanceGeneration {
	return newIndependentMaintenanceGeneration(engine, membershipOnlyTestRuntime{engine: engine}, maintenanceLaneTickers{
		membership: membershipTicker,
		source:     &manualCycleTicker{ticks: make(chan time.Time)},
		execution:  &manualCycleTicker{ticks: make(chan time.Time)},
	}, report, closers, finalizers)
}

func newManualMaintenanceLaneTickers() maintenanceLaneTickers {
	return maintenanceLaneTickers{
		membership: &manualCycleTicker{ticks: make(chan time.Time)},
		source:     &manualCycleTicker{ticks: make(chan time.Time)},
		execution:  &manualCycleTicker{ticks: make(chan time.Time)},
	}
}

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

func waitForMaintenance(t *testing.T, cycles <-chan struct{}) {
	t.Helper()
	<-cycles
}
