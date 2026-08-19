//go:build e2e

package daemon

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func assertApplicationStartupOperatorSnapshot(t *testing.T, application *e2eApplication) {
	t.Helper()
	assertApplicationReadyRendered(t, application.notifier.snapshot())
	assertApplicationNotificationPresent(t, application.notifier.snapshot(), "STATUS=degraded leadership=leader")
	assertApplicationLogSnapshot(t, application.logs.awaitRecord(t, "successful startup log", func(record e2eApplicationLogRecord) bool {
		return record.Operation == "generation" && record.Stage == "activation" && record.Outcome == "activated"
	}), "INFO|Runtime generation activated|progress_event|runtime|generation|"+
		"e2e-application-member|activation|activated|")
	assertApplicationLogSnapshot(t, application.logs.awaitRecord(t, "degraded inventory log", func(record e2eApplicationLogRecord) bool {
		return record.Operation == "refresh_inventory" && record.Outcome == "failed" && record.Type == "operational_event"
	}), "ERROR|Runner inventory refresh failed|operational_event|runner_inventory|refresh_inventory|"+
		"e2e-application-member|inventory|failed|operation")
}

func assertApplicationReadyRendered(t *testing.T, states []string) {
	t.Helper()
	for _, state := range states {
		if !strings.HasPrefix(state, "READY=1\nSTATUS=") {
			continue
		}
		status := strings.TrimPrefix(state, "READY=1\nSTATUS=")
		for _, rendered := range []string{
			"ready leadership=unknown",
			"degraded leadership=unknown",
			"degraded leadership=leader",
		} {
			if status == rendered {
				return
			}
		}
		t.Fatalf("startup READY notification has invalid rendered status %q", status)
	}
	t.Fatalf("operator notification snapshot has no rendered READY call; states=%q", states)
}

func assertApplicationRecoveryOperatorSnapshot(t *testing.T, application *e2eApplication) {
	t.Helper()
	assertApplicationNotificationPresent(t, application.notifier.snapshot(), "STATUS=operational leadership=leader")
	record := application.logs.awaitRecord(t, "inventory recovery log", func(record e2eApplicationLogRecord) bool {
		return record.Operation == "refresh_inventory" && record.Outcome == "recovered"
	})
	assertApplicationLogSnapshot(t, record, "INFO|Runner inventory refresh recovered|operational_event|"+
		"runner_inventory|refresh_inventory|e2e-application-member|inventory|recovered|")
	if record.FailureDuration == nil {
		t.Fatal("inventory recovery log does not contain failure_duration")
	}
}

func assertApplicationReloadOperatorSnapshot(t *testing.T, application *e2eApplication) {
	t.Helper()
	states := application.notifier.snapshot()
	reloading := 0
	for _, state := range states {
		if !strings.HasPrefix(state, "RELOADING=1\n") {
			continue
		}
		reloading++
		assertReloadingStateRendered(t, state)
	}
	if reloading != 2 {
		t.Fatalf("rendered reloading notifications = %d, want 2; states=%q", reloading, states)
	}
	for _, snapshot := range []struct {
		outcome string
		want    string
	}{
		{outcome: "rejected", want: "WARN|Runtime reload rejected|operational_event|runtime|reload|" +
			"e2e-application-member|activation|rejected|"},
		{outcome: "applied", want: "INFO|Runtime reload applied|operational_event|runtime|reload|" +
			"e2e-application-member|activation|applied|"},
	} {
		record := application.logs.awaitRecord(t, "reload "+snapshot.outcome+" log", func(record e2eApplicationLogRecord) bool {
			return record.Operation == "reload" && record.Outcome == snapshot.outcome
		})
		assertApplicationLogSnapshot(t, record, snapshot.want)
	}
}

func assertApplicationShutdownOperatorSnapshot(
	t *testing.T,
	application *e2eApplication,
	notificationCount int,
	logCount int,
) {
	t.Helper()
	states := application.notifier.snapshot()
	if len(states) != notificationCount {
		t.Fatalf("shutdown emitted service notifications %q, want no notifications", states[notificationCount:])
	}
	records := application.logs.records(t)
	if len(records) < logCount {
		t.Fatalf("application log record count decreased from %d to %d", logCount, len(records))
	}
	shutdownRecords := records[logCount:]
	want := []string{
		"INFO|Runtime generation started|progress_event|runtime|generation|e2e-application-member|deactivation|started|",
		"INFO|Runtime generation completed|progress_event|runtime|generation|e2e-application-member|deactivation|completed|",
		"INFO|Runtime generation started|progress_event|runtime|generation|e2e-application-member|shutdown|started|",
		"INFO|Runtime generation completed|progress_event|runtime|generation|e2e-application-member|shutdown|completed|",
	}
	if len(shutdownRecords) != len(want) {
		t.Fatalf("shutdown structured log count = %d, want %d; records=%+v", len(shutdownRecords), len(want), shutdownRecords)
	}
	for index, record := range shutdownRecords {
		assertApplicationLogSnapshot(t, record, want[index])
	}
}

func assertApplicationNotificationPresent(t *testing.T, states []string, want string) {
	t.Helper()
	for _, state := range states {
		if state == want {
			return
		}
	}
	t.Fatalf("operator notification snapshot does not contain %q; states=%q", want, states)
}

func assertReloadingStateRendered(t *testing.T, state string) {
	t.Helper()
	monotonic := strings.TrimPrefix(state, "RELOADING=1\n")
	if monotonic == "" {
		return
	}
	const prefix = "MONOTONIC_USEC="
	line := strings.TrimSuffix(monotonic, "\n")
	value := strings.TrimPrefix(line, prefix)
	if value == line {
		t.Fatalf("reloading notification = %q, want %s field", state, prefix)
	}
	if _, err := strconv.ParseUint(value, 10, 64); err != nil {
		t.Fatalf("reloading notification monotonic timestamp = %q: %v", value, err)
	}
}

func assertApplicationLogSnapshot(t *testing.T, record e2eApplicationLogRecord, want string) {
	t.Helper()
	observed := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s", record.Level, record.Message, record.Type,
		record.Component, record.Operation, record.Member, record.Stage, record.Outcome, record.ErrorCategory)
	if observed != want {
		t.Fatalf("structured operator log snapshot = %q, want %q", observed, want)
	}
}

func e2eApplicationMessageLogs(
	records []e2eApplicationLogRecord,
	runnerPool string,
	messageID int,
) []e2eApplicationLogRecord {
	relevant := make([]e2eApplicationLogRecord, 0)
	for _, record := range records {
		if record.RunnerPool == runnerPool && (record.MessageID == 0 || record.MessageID == messageID) &&
			(record.Operation == "source_poll" || record.Operation == "job_observation" ||
				record.Operation == "assigned_demand") {
			relevant = append(relevant, record)
		}
	}
	return relevant
}
