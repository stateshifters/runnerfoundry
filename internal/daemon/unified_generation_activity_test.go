package daemon

import (
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestUnifiedGenerationReportsImmediateAndScheduledMembershipActivity(t *testing.T) {
	t.Parallel()
	cycles := make(chan struct{}, 2)
	results := make(chan error, 2)
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	var activities []progressEventEnvelope
	generation := newTestMaintenanceGeneration(&controlledMaintenanceEngineFake{cycles: cycles, results: results}, ticker,
		captureProgress(&activities, "maintenance_membership"), nil, nil)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForMaintenance(t, cycles)
	results <- nil
	ticker.ticks <- time.Time{}
	waitForMaintenance(t, cycles)
	results <- nil
	mustCloseGeneration(t, generation)
	assertMembershipActivities(t, activities)
}

func captureProgress(activities *[]progressEventEnvelope, operation string) func(error) {
	return func(err error) {
		var activity progressEventEnvelope
		if errors.As(err, &activity) && activity.event.operation == operation {
			*activities = append(*activities, activity)
		}
	}
}

func assertMembershipActivities(t *testing.T, activities []progressEventEnvelope) {
	t.Helper()
	if len(activities) != 4 {
		t.Fatalf("membership activities = %#v, want four", activities)
	}
	if activities[0].level != slog.LevelInfo || activities[0].event.stage != "immediate" || activities[0].event.outcome != "started" {
		t.Fatalf("immediate start = %#v", activities[0])
	}
	if activities[2].level != slog.LevelDebug || activities[2].event.stage != "scheduled" || activities[2].event.outcome != "started" {
		t.Fatalf("scheduled start = %#v", activities[2])
	}
}

func TestUnifiedGenerationDeactivationCancelsFirstMembershipMaintenance(t *testing.T) {
	t.Parallel()
	cycles := make(chan struct{}, 1)
	reports := make(chan error, 1)
	generation := newTestMaintenanceGeneration(&controlledMaintenanceEngineFake{cycles: cycles, results: make(chan error)},
		&manualCycleTicker{ticks: make(chan time.Time)}, forwardMembershipReports(reports), nil, nil)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForMaintenance(t, cycles)
	mustDeactivateGeneration(t, generation)
	select {
	case err := <-reports:
		t.Fatalf("canceled membership maintenance was reported: %v", err)
	default:
	}
	mustCloseGeneration(t, generation)
}

func TestUnifiedGenerationReportsMembershipFailuresAndKeepsRunning(t *testing.T) {
	t.Parallel()
	maintenanceErr := errors.New("maintenance failed")
	cycles := make(chan struct{}, 2)
	reports := make(chan error, 2)
	maintenanceResults := make(chan error, 2)
	maintenanceResults <- maintenanceErr
	maintenanceResults <- nil
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	generation := newTestMaintenanceGeneration(&controlledMaintenanceEngineFake{cycles: cycles, results: maintenanceResults}, ticker,
		forwardMembershipReports(reports), nil, nil)
	mustPrepareGeneration(t, generation)
	mustActivateGeneration(t, generation)
	waitForMaintenance(t, cycles)
	if err := <-reports; !errors.Is(err, maintenanceErr) {
		t.Fatalf("reported error = %v", err)
	}
	ticker.ticks <- time.Time{}
	waitForMaintenance(t, cycles)
	if err := <-reports; err != nil {
		t.Fatalf("reported recovery = %v", err)
	}
	mustCloseGeneration(t, generation)
}

func forwardMembershipReports(reports chan<- error) func(error) {
	return func(err error) {
		var event operationalEventEnvelope
		if errors.As(err, &event) && event.event.operation == "maintenance_lane" && event.event.stage == "membership" {
			reports <- event.cause
		}
	}
}
