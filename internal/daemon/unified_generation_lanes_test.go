package daemon

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIndependentMaintenanceLanesReportFailureAndRecoveryPerLane(t *testing.T) {
	wantErr := errors.New("source unavailable")
	fixture := newReportingLaneFixture(wantErr)

	assertNoTestError(t, fixture.generation.Prepare(context.Background()), "prepare generation")
	assertNoTestError(t, fixture.generation.Activate(context.Background()), "activate generation")
	awaitTestSignal(t, fixture.sourceCalls, "failed source maintenance")
	failure := awaitMaintenanceLaneEvent(t, fixture.events)
	if failure.outcome != "failed" || !errors.Is(failure.err, wantErr) {
		t.Fatalf("source failure event = %#v", failure)
	}
	fixture.sourceTicker.ticks <- time.Time{}
	awaitTestSignal(t, fixture.sourceCalls, "recovered source maintenance")
	recovery := awaitMaintenanceLaneEvent(t, fixture.events)
	if recovery.outcome != "recovered" || recovery.err != nil {
		t.Fatalf("source recovery event = %#v", recovery)
	}
	assertNoTestError(t, fixture.generation.Close(context.Background()), "close generation")
}

type reportingLaneFixture struct {
	generation   *maintenanceGeneration
	sourceTicker *manualCycleTicker
	sourceCalls  <-chan struct{}
	events       <-chan operationalEvent
}

func newReportingLaneFixture(firstResult error) reportingLaneFixture {
	sourceCalls := make(chan struct{}, 2)
	sourceResults := make(chan error, 2)
	sourceResults <- firstResult
	sourceResults <- nil
	engine := &maintenanceLaneEngineFake{
		membership: func(context.Context) error { return nil },
		source: func(context.Context) error {
			sourceCalls <- struct{}{}
			return <-sourceResults
		},
		execution: func(context.Context) error { return nil },
	}
	sourceTicker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	events := make(chan operationalEvent, 2)
	generation := newIndependentMaintenanceGeneration(engine, engine, maintenanceLaneTickers{
		membership: &manualCycleTicker{ticks: make(chan time.Time)}, source: sourceTicker,
		execution: &manualCycleTicker{ticks: make(chan time.Time)},
	}, captureSourceLaneEvents(events), nil, nil)
	return reportingLaneFixture{generation: generation, sourceTicker: sourceTicker, sourceCalls: sourceCalls, events: events}
}

func captureSourceLaneEvents(events chan<- operationalEvent) func(error) {
	return func(err error) {
		var envelope operationalEventEnvelope
		if errors.As(err, &envelope) && envelope.event.operation == "maintenance_lane" &&
			envelope.event.stage == "source" {
			events <- envelope.event
		}
	}
}

func awaitMaintenanceLaneEvent(t *testing.T, events <-chan operationalEvent) operationalEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for maintenance lane event")
		return operationalEvent{}
	}
}

func TestIndependentMaintenanceLanesStartWhileMembershipIsBlocked(t *testing.T) {
	fixture := newBlockedMembershipGeneration()

	assertNoTestError(t, fixture.generation.Prepare(context.Background()), "prepare generation")
	assertNoTestError(t, fixture.generation.Activate(context.Background()), "activate generation")
	awaitTestSignal(t, fixture.membershipStarted, "membership lane start")
	awaitTestSignal(t, fixture.sourceStarted, "source lane start")
	awaitTestSignal(t, fixture.executionStarted, "execution lane start")
	close(fixture.releaseMembership)
	assertNoTestError(t, fixture.generation.Close(context.Background()), "close generation")
}

func TestMaintenanceLaneCancellationDoesNotReportFailure(t *testing.T) {
	reported := false
	generation := &maintenanceGeneration{report: func(error) { reported = true }}
	generation.reportLane("source", context.Canceled)
	if reported {
		t.Fatal("lane cancellation was reported as an operational failure")
	}
}

func TestMaintenanceLaneCompletionObserverRunsAfterFailedLane(t *testing.T) {
	wantErr := errors.New("source unavailable")
	ticker := &manualCycleTicker{ticks: make(chan time.Time)}
	laneReturned := false
	observerCalls := 0
	generation := &maintenanceGeneration{
		currentTime: time.Now,
		report:      func(error) {},
		maintenanceLaneCompleted: func(laneName string, observedTicker cycleTicker) {
			observerCalls++
			if !laneReturned {
				t.Error("maintenance completion was observed before the lane returned")
			}
			if laneName != "source" || observedTicker != ticker {
				t.Errorf("observed lane = (%q, %T), want source and its configured ticker", laneName, observedTicker)
			}
		},
	}
	lane := scheduledMaintenanceLane{name: "source", ticker: ticker, run: func(context.Context) error {
		laneReturned = true
		return wantErr
	}}

	generation.runCycle(context.Background(), lane, "scheduled", 0)

	if observerCalls != 1 {
		t.Fatalf("maintenance completion observer calls = %d, want 1", observerCalls)
	}
}

type blockedMembershipFixture struct {
	generation        *maintenanceGeneration
	membershipStarted <-chan struct{}
	releaseMembership chan<- struct{}
	sourceStarted     <-chan struct{}
	executionStarted  <-chan struct{}
}

func newBlockedMembershipGeneration() blockedMembershipFixture {
	membershipStarted := make(chan struct{})
	releaseMembership := make(chan struct{})
	sourceStarted := make(chan struct{})
	executionStarted := make(chan struct{})
	engine := &maintenanceLaneEngineFake{
		membership: func(context.Context) error {
			close(membershipStarted)
			<-releaseMembership
			return nil
		},
		source:    func(context.Context) error { close(sourceStarted); return nil },
		execution: func(context.Context) error { close(executionStarted); return nil },
	}
	tickers := maintenanceLaneTickers{
		membership: &manualCycleTicker{ticks: make(chan time.Time)},
		source:     &manualCycleTicker{ticks: make(chan time.Time)},
		execution:  &manualCycleTicker{ticks: make(chan time.Time)},
	}
	generation := newIndependentMaintenanceGeneration(engine, engine, tickers, nil, nil, nil)
	return blockedMembershipFixture{
		generation: generation, membershipStarted: membershipStarted, releaseMembership: releaseMembership,
		sourceStarted: sourceStarted, executionStarted: executionStarted,
	}
}

func awaitTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type maintenanceLaneEngineFake struct {
	membership func(context.Context) error
	source     func(context.Context) error
	execution  func(context.Context) error
}

func (*maintenanceLaneEngineFake) Activate() error                  { return nil }
func (*maintenanceLaneEngineFake) Deactivate(context.Context) error { return nil }
func (*maintenanceLaneEngineFake) Close(context.Context) error      { return nil }

func (fake *maintenanceLaneEngineFake) MaintainMembership(ctx context.Context) error {
	return fake.membership(ctx)
}

func (fake *maintenanceLaneEngineFake) MaintainSource(ctx context.Context) error {
	return fake.source(ctx)
}

func (fake *maintenanceLaneEngineFake) MaintainExecution(ctx context.Context) error {
	return fake.execution(ctx)
}
