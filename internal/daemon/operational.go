package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
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
	operation     string
	outcome       string
	runnerPool    core.RunnerPool
	jobID         core.JobID
	count         int
	capacity      int
	leadership    operationalLeadership
	err           error
	errorCategory string
}

type operationalEventEnvelope struct {
	event operationalEvent
	cause error
}

func (envelope operationalEventEnvelope) Error() string {
	return "operational event: " + envelope.event.operation
}
func (envelope operationalEventEnvelope) Unwrap() error { return envelope.cause }

func reportOperational(report func(error), event operationalEvent) {
	if report != nil {
		report(operationalEventEnvelope{event: event, cause: event.err})
	}
}

func reportOperationalError(report func(error), event operationalEvent, cause error) {
	if report != nil {
		report(operationalEventEnvelope{event: event, cause: cause})
	}
}

type operationFailure struct {
	fingerprint string
	first       time.Time
	lastLog     time.Time
	count       int
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

func (reporter *productionRuntimeReporter) reportEvent(now time.Time, event operationalEvent) {
	if event.err != nil {
		reporter.reportFailure(now, event)
		return
	}
	if event.outcome == "recovered" {
		reporter.reportRecovery(now, event)
		return
	}
	if _, failed := reporter.failures[event.operation]; failed {
		reporter.reportRecovery(now, operationalEvent{operation: event.operation, outcome: "recovered"})
	}
	level := slog.LevelInfo
	if event.outcome == "rejected" {
		level = slog.LevelWarn
	}
	reporter.logEvent(level, event, nil)
}

func (reporter *productionRuntimeReporter) reportFailure(now time.Time, event operationalEvent) {
	fingerprint := fmt.Sprintf("%T:%s", event.err, event.err.Error())
	failure, exists := reporter.failures[event.operation]
	changed := !exists || failure.fingerprint != fingerprint
	if changed {
		failure = operationFailure{fingerprint: fingerprint, first: now}
	}
	failure.count++
	shouldLog := changed || now.Sub(failure.lastLog) >= 5*time.Minute
	if shouldLog {
		failure.lastLog = now
		extra := []slog.Attr{slog.Int("recurrence_count", failure.count)}
		reporter.logEvent(slog.LevelError, event, extra)
	}
	reporter.failures[event.operation] = failure
}

func (reporter *productionRuntimeReporter) reportRecovery(now time.Time, event operationalEvent) {
	failure, exists := reporter.failures[event.operation]
	if !exists {
		return
	}
	delete(reporter.failures, event.operation)
	extra := []slog.Attr{slog.Int("recurrence_count", failure.count),
		slog.Duration("failure_duration", now.Sub(failure.first))}
	reporter.logEvent(slog.LevelInfo, event, extra)
}

func (reporter *productionRuntimeReporter) logEvent(level slog.Level, event operationalEvent, extra []slog.Attr) {
	attributes := []slog.Attr{slog.String("operation", event.operation), slog.String("member", string(reporter.member)),
		slog.String("outcome", event.outcome)}
	if event.runnerPool != "" {
		attributes = append(attributes, slog.String("runner_pool", string(event.runnerPool)))
	}
	if event.jobID != (core.JobID{}) {
		attributes = append(attributes, slog.String("job_id", event.jobID.String()))
	}
	if event.count != 0 {
		attributes = append(attributes, slog.Int("count", event.count))
	}
	if event.capacity != 0 {
		attributes = append(attributes, slog.Int("capacity", event.capacity))
	}
	if event.leadership != "" {
		attributes = append(attributes, slog.String("leadership", string(event.leadership)))
	}
	if event.err != nil {
		attributes = append(attributes, slog.String("error_category", classifyOperationalError(event)))
	}
	attributes = append(attributes, extra...)
	reporter.logging.Logger().LogAttrs(context.Background(), level, "RunnerFoundry operational event", attributes...)
}

func classifyOperationalError(event operationalEvent) string {
	if event.errorCategory != "" {
		return event.errorCategory
	}
	if errors.Is(event.err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(event.err, context.DeadlineExceeded) {
		return "deadline"
	}
	return "operation"
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
