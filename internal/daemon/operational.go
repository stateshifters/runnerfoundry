package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

type operationalHealth string
type operationalLeadership string

const (
	healthOperational operationalHealth     = "operational"
	healthDegraded    operationalHealth     = "degraded"
	leadershipUnknown operationalLeadership = "unknown"
	leadershipStandby operationalLeadership = "standby"
	leadershipLeader  operationalLeadership = "leader"
)

type operationalStatus struct {
	health     operationalHealth
	leadership operationalLeadership
}

type operationalEvent struct {
	component          string
	operation          string
	stage              string
	outcome            string
	runnerPool         core.RunnerPool
	jobID              core.JobID
	runnerID           string
	runnerName         core.RunnerName
	repository         string
	endpoint           string
	diagnostic         string
	sourceRequestID    int64
	sourceRequestIDSet bool
	installationID     int64
	runnerGroupID      int64
	scaleSetID         int64
	messageID          int64
	githubEventType    string
	githubJobID        string
	workflowRunID      int64
	githubRunnerID     int64
	identityReason     string
	count              int
	desiredJobs        int
	availableJobs      int
	acquiredJobs       int
	runningJobs        int
	registeredRunners  int
	busyRunners        int
	idleRunners        int
	activeJobs         int
	queuedJobs         int
	processingJobs     int
	jobsAdded          int
	jobsRemoved        int
	repositoryCount    int
	installationCount  int
	current            int
	total              int
	capacity           int
	duration           time.Duration
	leadership         operationalLeadership
	err                error
	errorCategory      string
	provider           string
	composeService     string
	stateBackend       string
	stateOperation     string
	sourcePC           uintptr
}

type operationalEventEnvelope struct {
	event operationalEvent
	cause error
}

type progressEventEnvelope struct {
	level slog.Level
	event operationalEvent
}

type maintenanceCycleResult struct{ cause error }

func (result maintenanceCycleResult) Error() string {
	return "maintenance cycle contained reported failures"
}
func (result maintenanceCycleResult) Unwrap() error { return result.cause }

type reportedOperationalError struct{ cause error }

func (reported reportedOperationalError) Error() string { return reported.cause.Error() }
func (reported reportedOperationalError) Unwrap() error { return reported.cause }

func markOperationallyReported(err error) error {
	if err == nil {
		return nil
	}
	return reportedOperationalError{cause: err}
}

func operationalErrorWasReported(err error) bool {
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return everyOperationalErrorWasReported(joined.Unwrap())
	}
	var reported reportedOperationalError
	return errors.As(err, &reported)
}

func everyOperationalErrorWasReported(errs []error) bool {
	for _, err := range errs {
		if !operationalErrorWasReported(err) {
			return false
		}
	}
	return true
}

func (envelope progressEventEnvelope) Error() string {
	return "progress event: " + envelope.event.operation
}

func (envelope operationalEventEnvelope) Error() string {
	return "operational event: " + envelope.event.operation
}
func (envelope operationalEventEnvelope) Unwrap() error { return envelope.cause }

func reportOperational(report func(error), event operationalEvent) {
	if report != nil {
		event.captureSource(1)
		report(operationalEventEnvelope{event: event, cause: event.err})
	}
}

func reportOperationalError(report func(error), event operationalEvent, cause error) {
	if report != nil {
		event.captureSource(1)
		report(operationalEventEnvelope{event: event, cause: cause})
	}
}

func reportProgress(report func(error), level slog.Level, event operationalEvent) {
	if report != nil {
		event.captureSource(1)
		report(progressEventEnvelope{level: level, event: event})
	}
}

func (event *operationalEvent) captureSource(skip int) {
	if event.sourcePC != 0 {
		return
	}
	programCounters := make([]uintptr, 1)
	if runtime.Callers(skip+2, programCounters) != 0 {
		event.sourcePC = programCounters[0]
	}
}

type operationFailure struct {
	first time.Time
	event operationalEvent
}

type productionRuntimeReporter struct {
	logging    *loggingManager
	member     core.MemberID
	now        func() time.Time
	notifier   serviceNotifier
	mutex      sync.Mutex
	failures   map[string]operationFailure
	leadership operationalLeadership
	lastStatus operationalStatus
}

func newProductionRuntimeReporter(
	logging *loggingManager,
	state *productionState,
	now func() time.Time,
) *productionRuntimeReporter {
	return &productionRuntimeReporter{logging: logging, member: state.member, now: now,
		failures: make(map[string]operationFailure), leadership: leadershipUnknown}
}

func (reporter *productionRuntimeReporter) reportMaintenance(err error) {
	var cycleResult maintenanceCycleResult
	if errors.As(err, &cycleResult) {
		return
	}
	var progress progressEventEnvelope
	if errors.As(err, &progress) {
		reporter.ReportProgress(progress.level, progress.event)
		return
	}
	var envelope operationalEventEnvelope
	if errors.As(err, &envelope) {
		reporter.Report(envelope.event)
		return
	}
	if err == nil {
		reporter.Report(operationalEvent{operation: "maintenance", outcome: "recovered"})
		return
	}
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: err})
}

func (reporter *productionRuntimeReporter) Report(event operationalEvent) {
	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()
	now := reporter.now()
	if event.leadership != "" {
		reporter.leadership = event.leadership
	}
	reporter.reportEvent(now, event)
	reporter.publishStatus()
}

func (reporter *productionRuntimeReporter) ReportProgress(level slog.Level, event operationalEvent) {
	reporter.writeEvent(level, "progress_event", event, nil)
}

func (reporter *productionRuntimeReporter) reportEvent(now time.Time, event operationalEvent) {
	if event.err != nil {
		reporter.reportFailure(now, event)
		return
	}
	if event.outcome == "recovered" {
		reporter.reportRecovery(now, event)
		return
	}
	if event.outcome == "ignored" {
		reporter.logEvent(slog.LevelWarn, event, nil)
		return
	}
	if _, failed := reporter.failures[operationalFailureKey(event)]; failed {
		recovery := event
		recovery.outcome = "recovered"
		recovery.err = nil
		recovery.diagnostic = ""
		reporter.reportRecovery(now, recovery)
	}
	level := slog.LevelInfo
	if event.outcome == "rejected" {
		level = slog.LevelWarn
	}
	reporter.logEvent(level, event, nil)
}

func (reporter *productionRuntimeReporter) reportFailure(now time.Time, event operationalEvent) {
	key := operationalFailureKey(event)
	if _, exists := reporter.failures[key]; !exists {
		reporter.failures[key] = operationFailure{first: now, event: event}
	}
	reporter.logEvent(slog.LevelError, event, nil)
}

func (reporter *productionRuntimeReporter) reportRecovery(now time.Time, event operationalEvent) {
	key := operationalFailureKey(event)
	failure, exists := reporter.failures[key]
	if !exists {
		if event.stage == "" && event.repository == "" && event.runnerPool == "" && event.endpoint == "" {
			reporter.recoverOperation(now, event)
		}
		return
	}
	delete(reporter.failures, key)
	extra := []slog.Attr{slog.Duration("failure_duration", now.Sub(failure.first))}
	reporter.logEvent(slog.LevelInfo, event, extra)
}

func (reporter *productionRuntimeReporter) recoverOperation(now time.Time, event operationalEvent) {
	for key, failure := range reporter.failures {
		if failure.event.operation != event.operation {
			continue
		}
		delete(reporter.failures, key)
		recovery := failure.event
		recovery.err = nil
		recovery.diagnostic = ""
		recovery.outcome = "recovered"
		reporter.logEvent(slog.LevelInfo, recovery,
			[]slog.Attr{slog.Duration("failure_duration", now.Sub(failure.first))})
	}
}

func operationalFailureKey(event operationalEvent) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s", event.component, event.operation, event.stage,
		event.repository, event.runnerPool, event.provider, event.endpoint, event.stateBackend, event.stateOperation)
}

func (reporter *productionRuntimeReporter) logEvent(level slog.Level, event operationalEvent, extra []slog.Attr) {
	reporter.writeEvent(level, "operational_event", event, extra)
}

func (reporter *productionRuntimeReporter) publishStatus() {
	health := healthOperational
	if len(reporter.failures) != 0 {
		health = healthDegraded
	}
	status := operationalStatus{health: health, leadership: reporter.leadership}
	if reporter.notifier == nil || status == reporter.lastStatus {
		return
	}
	reporter.lastStatus = status
	if err := reporter.notifier.Status(context.Background(), status); err != nil {
		reporter.logging.Logger().ErrorContext(context.Background(), "RunnerFoundry status notification failed",
			slog.String("operation", "status"), slog.String("outcome", "failed"),
			slog.String("error_category", classifyOperationalError(operationalEvent{err: err})))
	}
}
