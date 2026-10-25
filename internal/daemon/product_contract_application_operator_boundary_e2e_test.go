//go:build e2e

package daemon

import (
	"context"
	"errors"
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
	records := application.logs.records(t)
	if len(records) < logCount {
		t.Fatalf("application log record count decreased from %d to %d", logCount, len(records))
	}
	shutdownRecords := records[logCount:]
	assertApplicationNoShutdownNotifications(t, application, states, notificationCount, shutdownRecords)
	want := []string{
		"INFO|Runtime generation started|progress_event|runtime|generation|e2e-application-member|deactivation|started|",
		"INFO|Runtime generation completed|progress_event|runtime|generation|e2e-application-member|deactivation|completed|",
		"INFO|Runtime generation started|progress_event|runtime|generation|e2e-application-member|shutdown|started|",
		"INFO|Runtime generation completed|progress_event|runtime|generation|e2e-application-member|shutdown|completed|",
	}
	lifecycleRecords := applicationShutdownGenerationRecords(t, shutdownRecords)
	if len(lifecycleRecords) != len(want) {
		t.Fatalf("shutdown generation log count = %d, want %d; records=%+v", len(lifecycleRecords), len(want), shutdownRecords)
	}
	for index, record := range lifecycleRecords {
		assertApplicationLogSnapshot(t, record, want[index])
	}
	assertApplicationLogSnapshot(t, shutdownRecords[len(shutdownRecords)-1], want[len(want)-1])
}

func assertApplicationNoShutdownNotifications(
	t *testing.T,
	application *e2eApplication,
	states []string,
	notificationCount int,
	shutdownRecords []e2eApplicationLogRecord,
) {
	t.Helper()
	if len(states) != notificationCount {
		application.runtime.reporter.mutex.Lock()
		lastStatus := application.runtime.reporter.lastStatus
		failures := make([]string, 0, len(application.runtime.reporter.failures))
		for _, failure := range application.runtime.reporter.failures {
			failures = append(failures, applicationFailureClassification(failure.event))
		}
		application.runtime.reporter.mutex.Unlock()
		t.Fatalf("shutdown emitted service notifications %q, want no notifications; status=%+v failures=%v records=%+v",
			states[notificationCount:], lastStatus, failures, shutdownRecords)
	}
}

func applicationFailureClassification(event operationalEvent) string {
	types := make([]string, 0, 4)
	for cause := event.err; cause != nil; cause = errors.Unwrap(cause) {
		types = append(types, fmt.Sprintf("%T", cause))
	}
	var sqliteCode interface{ Code() int }
	code := -1
	if errors.As(event.err, &sqliteCode) {
		code = sqliteCode.Code()
	}
	return fmt.Sprintf("%s/%s/%s canceled=%t deadline=%t sqlite_code=%d types=%v",
		event.operation, event.stage, event.outcome, errors.Is(event.err, context.Canceled),
		errors.Is(event.err, context.DeadlineExceeded), code, types)
}

func applicationShutdownGenerationRecords(t *testing.T, records []e2eApplicationLogRecord) []e2eApplicationLogRecord {
	t.Helper()
	generationRecords := make([]e2eApplicationLogRecord, 0, len(records))
	for _, record := range records {
		if record.Component == "runtime" && record.Operation == "generation" {
			generationRecords = append(generationRecords, record)
			continue
		}
		if !applicationShutdownSourceProgress(record) {
			t.Fatalf("unexpected shutdown log record: %+v", record)
		}
	}
	return generationRecords
}

func applicationShutdownSourceProgress(record e2eApplicationLogRecord) bool {
	if record.Level != "INFO" || record.Type != "progress_event" || record.Component != "github_registration" ||
		record.Operation != "source_reconciliation" || record.Member != "e2e-application-member" ||
		record.ErrorCategory != "" || record.RunnerPool != "" || record.MessageID != 0 {
		return false
	}
	switch record.Stage {
	case "discover_repositories", "configure_repositories", "reconciliation":
		return record.Outcome == "started" || record.Outcome == "completed"
	default:
		return false
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
