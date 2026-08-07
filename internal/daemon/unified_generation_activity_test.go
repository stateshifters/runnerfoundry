package daemon

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestUnifiedGenerationReportsImmediateAndScheduledCycleActivity(t *testing.T) {
	t.Parallel()
	cycles := make(chan struct{}, 2)
	results := make(chan error, 2)
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	var activities []progressEventEnvelope
	generation := newMaintenanceGeneration(&controlledCycleEngineFake{cycles: cycles, results: results}, ticker,
		captureProgress(&activities, "maintenance_cycle"), nil, nil)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForCycle(t, cycles)
	results <- nil
	ticker.ticks <- time.Time{}
	waitForCycle(t, cycles)
	results <- nil
	mustCloseGeneration(t, generation)
	assertCycleActivities(t, activities)
}

func captureProgress(activities *[]progressEventEnvelope, operation string) func(error) {
	return func(err error) {
		var activity progressEventEnvelope
		if errors.As(err, &activity) && activity.event.operation == operation {
			*activities = append(*activities, activity)
		}
	}
}

func assertCycleActivities(t *testing.T, activities []progressEventEnvelope) {
	t.Helper()
	if len(activities) != 4 {
		t.Fatalf("cycle activities = %#v, want four", activities)
	}
	if activities[0].level != slog.LevelInfo || activities[0].event.stage != "immediate" || activities[0].event.outcome != "started" {
		t.Fatalf("immediate start = %#v", activities[0])
	}
	if activities[2].level != slog.LevelDebug || activities[2].event.stage != "scheduled" || activities[2].event.outcome != "started" {
		t.Fatalf("scheduled start = %#v", activities[2])
	}
}

func TestUnifiedGenerationDeactivationCancelsFirstMaintenanceCycle(t *testing.T) {
	t.Parallel()
	cycles := make(chan struct{}, 1)
	reports := make(chan error, 1)
	generation := newMaintenanceGeneration(&controlledCycleEngineFake{cycles: cycles, results: make(chan error)},
		&manualCycleTicker{ticks: make(chan time.Time)}, forwardCycleReports(reports), nil, nil)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForCycle(t, cycles)
	mustDeactivateGeneration(t, generation)
	if err := <-reports; !errors.Is(err, context.Canceled) {
		t.Fatalf("reported cancellation = %v", err)
	}
	mustCloseGeneration(t, generation)
}

func TestUnifiedGenerationReportsCycleFailuresAndKeepsRunning(t *testing.T) {
	t.Parallel()
	cycleErr := errors.New("cycle failed")
	cycles := make(chan struct{}, 2)
	reports := make(chan error, 2)
	cycleResults := make(chan error, 2)
	cycleResults <- cycleErr
	cycleResults <- nil
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newMaintenanceGeneration(&controlledCycleEngineFake{cycles: cycles, results: cycleResults}, ticker,
		forwardCycleReports(reports), nil, nil)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForCycle(t, cycles)
	if err := <-reports; !errors.Is(err, cycleErr) {
		t.Fatalf("reported error = %v", err)
	}
	ticker.ticks <- time.Time{}
	waitForCycle(t, cycles)
	if err := <-reports; err != nil {
		t.Fatalf("reported recovery = %v", err)
	}
	mustCloseGeneration(t, generation)
}

func forwardCycleReports(reports chan<- error) func(error) {
	return func(err error) {
		var progress progressEventEnvelope
		if !errors.As(err, &progress) {
			reports <- err
		}
	}
}
