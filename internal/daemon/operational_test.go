package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestOperationalReporterPublishesHealthAndRateLimitedTextEvents(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, now := newOperationalReporterFixture(t, "text", destination)
	notifier := &recordingStatusNotifier{}
	reporter.notifier = notifier
	exerciseOperationalRateLimiting(reporter, now)
	assertOperationalTextSnapshot(t, destination.String())
	assertOperationalStatusSnapshot(t, notifier.statuses)
}

func exerciseOperationalRateLimiting(reporter *productionRuntimeReporter, now *time.Time) {
	reporter.Report(operationalEvent{operation: "leadership", outcome: "acquired", leadership: leadershipLeader})
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("secret detail")})
	*now = (*now).Add(time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("secret detail")})
	*now = (*now).Add(5 * time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("secret detail")})
	*now = (*now).Add(time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("changed detail")})
	*now = (*now).Add(time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "recovered"})
}

func assertOperationalTextSnapshot(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "secret detail") || strings.Contains(output, "changed detail") {
		t.Fatalf("log exposed error detail: %q", output)
	}
	for _, snapshot := range []string{
		"operation=leadership member=member-a outcome=acquired leadership=leader",
		"operation=maintenance member=member-a outcome=failed error_category=operation recurrence_count=1",
		"operation=maintenance member=member-a outcome=failed error_category=operation recurrence_count=3",
		"operation=maintenance member=member-a outcome=recovered recurrence_count=1 failure_duration=1m0s",
	} {
		if !strings.Contains(output, snapshot) {
			t.Fatalf("text log snapshot %q missing from %q", snapshot, output)
		}
	}
}

func assertOperationalStatusSnapshot(t *testing.T, statuses []operationalStatus) {
	t.Helper()
	wantStatuses := []operationalStatus{
		{health: healthOperational, leadership: leadershipLeader},
		{health: healthDegraded, leadership: leadershipLeader},
		{health: healthOperational, leadership: leadershipLeader},
	}
	if len(statuses) != len(wantStatuses) {
		t.Fatalf("statuses = %#v, want %#v", statuses, wantStatuses)
	}
	for index := range wantStatuses {
		if statuses[index] != wantStatuses[index] {
			t.Fatalf("status %d = %#v, want %#v", index, statuses[index], wantStatuses[index])
		}
	}
}

func TestOperationalReporterPublishesStructuredJSONIdentifiers(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, _ := newOperationalReporterFixture(t, "json", destination)
	jobID := core.JobID{1}
	reporter.Report(operationalEvent{operation: "runner_retry", outcome: "source_terminal",
		runnerPool: "linux", jobID: jobID, count: 2, capacity: 3})

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(destination.String())), &record); err != nil {
		t.Fatalf("decode JSON log: %v", err)
	}
	want := map[string]any{"operation": "runner_retry", "member": "member-a", "outcome": "source_terminal",
		"runner_pool": "linux", "job_id": jobID.String(), "count": float64(2), "capacity": float64(3)}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("JSON field %q = %#v, want %#v in %#v", key, record[key], value, record)
		}
	}
}

func TestOperationalReporterHandlesEnvelopesRecoveryAndNotificationFailure(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, _ := newOperationalReporterFixture(t, "text", destination)
	reporter.notifier = &failingStatusNotifier{}
	reporter.reportMaintenance(operationalEventEnvelope{event: operationalEvent{operation: "reload", outcome: "rejected"}})
	reporter.Report(operationalEvent{operation: "claim", outcome: "failed", err: errors.New("first")})
	reporter.Report(operationalEvent{operation: "claim", outcome: "claimed"})
	reporter.Report(operationalEvent{operation: "unknown", outcome: "recovered"})
	reporter.Report(operationalEvent{operation: "explicit", outcome: "failed", err: errors.New("hidden"), errorCategory: "protocol"})

	output := destination.String()
	for _, want := range []string{"operation=reload", "outcome=rejected", "operation=claim", "outcome=recovered",
		"error_category=protocol", "operation=status"} {
		if !strings.Contains(output, want) {
			t.Fatalf("log %q does not contain %q", output, want)
		}
	}
	if (operationalEventEnvelope{event: operationalEvent{operation: "reload"}}).Error() != "operational event: reload" {
		t.Fatal("unexpected envelope description")
	}
}

func TestPoolOperationalEventsDescribeOutcomesAndCleanup(t *testing.T) {
	events := make([]operationalEvent, 0)
	report := func(err error) {
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("report = %T, want operational envelope", err)
		}
		events = append(events, envelope.event)
	}
	jobID := core.JobID{1}
	reportProcessOutcome(report, "linux", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRunnerStarted, JobID: jobID}, nil)
	reportProcessOutcome(report, "linux", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry,
		RetryOutcome: core.RetrySourceTerminal, JobID: jobID}, errors.New("retry"))
	reportProcessOutcome(report, "linux", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil)
	reportCleanup(report, "linux", runtimeengine.PoolReconciliation{})
	reportCleanup(report, "linux", runtimeengine.PoolReconciliation{RemovedRunners: 1, RemovedProcessing: 1})
	if len(events) != 5 || events[0].operation != "claim" || events[1].operation != "runner_start" ||
		events[2].operation != "claim" || events[3].operation != "runner_retry" || events[4].operation != "cleanup" {
		t.Fatalf("events = %#v", events)
	}
}

func newOperationalReporterFixture(
	t *testing.T,
	format string,
	destination *memoryLogDestination,
) (*productionRuntimeReporter, *time.Time) {
	t.Helper()
	logging, err := newLoggingManager(config.LogConfig{Format: format}, singleLogDestinationFactory{destination: destination})
	if err != nil {
		t.Fatalf("configure logging: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := logging.Close(); closeErr != nil {
			t.Errorf("close logging: %v", closeErr)
		}
	})
	now := time.Unix(1_700_000_000, 0).UTC()
	reporter := newProductionRuntimeReporter(logging, &productionState{member: "member-a"},
		func() time.Time { return now })
	return reporter, &now
}

type recordingStatusNotifier struct{ statuses []operationalStatus }

func (*recordingStatusNotifier) Ready(context.Context) error     { return nil }
func (*recordingStatusNotifier) Reloading(context.Context) error { return nil }
func (notifier *recordingStatusNotifier) Status(_ context.Context, status operationalStatus) error {
	notifier.statuses = append(notifier.statuses, status)
	return nil
}

type failingStatusNotifier struct{}

func (*failingStatusNotifier) Ready(context.Context) error     { return nil }
func (*failingStatusNotifier) Reloading(context.Context) error { return nil }
func (*failingStatusNotifier) Status(context.Context, operationalStatus) error {
	return errors.New("notify failed")
}
