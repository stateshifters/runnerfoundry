//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestProductContractGitHubMessageClassesSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseGitHubMessageClasses(t, config.StateConfig{Path: ":memory:"}, "e2e-sqlite-message-matrix")
}

func TestProductContractGitHubMessageClassesPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	exerciseGitHubMessageClasses(
		t, config.StateConfig{Postgres: &settings}, "e2e-postgres-message-matrix",
	)
}

func exerciseGitHubMessageClasses(t *testing.T, state config.StateConfig, memberName string) {
	t.Helper()
	harness := newE2EHarness(t, state, memberName, false)
	harness.establishSourceSession()
	session := harness.host.sessions["linux"]
	started := deliverDuplicateAvailableMessage(t, harness, session)
	deliverRunnerNameStartedWithAckReplay(t, harness, session, started)
	deliverRunnerNameCompletedWithAckReplay(t, harness, session, started)
	deliverIgnoredGitHubHistory(t, harness, session)
}

func deliverDuplicateAvailableMessage(
	t *testing.T,
	harness *e2eHarness,
	session *e2eGitHubSession,
) execution.StartRequest {
	t.Helper()
	base := upstream.JobMessageBase{RunnerRequestID: 1901, QueueTime: harness.currentTime.AddDate(-10, 0, 0)}
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{
			{JobMessageBase: base},
			{JobMessageBase: base},
		},
	})
	requested := awaitAcquisitionIDs(t, session)
	if len(requested) != 1 || requested[0] != 1901 {
		t.Fatalf("duplicate JobAvailable acquisition IDs = %v, want [1901]", requested)
	}
	started := awaitProviderStart(t, "duplicate JobAvailable provider start", harness.provider.starts)
	if attempts := harness.provider.attemptCount(); attempts != 1 {
		t.Fatalf("duplicate JobAvailable provider attempts = %d, want 1", attempts)
	}
	return started
}

func deliverRunnerNameStartedWithAckReplay(
	t *testing.T,
	harness *e2eHarness,
	session *e2eGitHubSession,
	started execution.StartRequest,
) {
	t.Helper()
	message := &upstream.RunnerScaleSetMessage{
		Statistics:         e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName)}},
	}
	deliverWithAcknowledgementReplay(t, harness, session, message, func() {
		assertMessageMatrixStarted(t, harness, started.JobID)
	})
	assertMessageMatrixStarted(t, harness, started.JobID)
}

func deliverRunnerNameCompletedWithAckReplay(
	t *testing.T,
	harness *e2eHarness,
	session *e2eGitHubSession,
	started execution.StartRequest,
) {
	t.Helper()
	message := &upstream.RunnerScaleSetMessage{
		Statistics:           e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(started.RunnerName), Result: "failed"}},
	}
	deliverWithAcknowledgementReplay(t, harness, session, message, func() {
		assertMessageMatrixTerminal(t, harness, started.JobID)
	})
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "message-matrix terminal cleanup", harness.provider.destroys)
	harness.completeMaintenanceCycle(harness.execution)
	assertExactDemandState(t, harness, 1901, core.QueueSourceTerminal)
}

func deliverWithAcknowledgementReplay(
	t *testing.T,
	harness *e2eHarness,
	session *e2eGitHubSession,
	message *upstream.RunnerScaleSetMessage,
	assertDurableEffect func(),
) {
	t.Helper()
	drainIntegers(session.ackAttempts)
	messageID := session.assignMessageID(message)
	wantErr := errors.New("scripted lifecycle acknowledgement failure")
	session.failNextAcknowledgement(wantErr)
	session.messages <- message
	if attempt := awaitInteger(t, "lifecycle acknowledgement failure", session.ackAttempts); attempt != messageID {
		t.Fatalf("lifecycle acknowledgement attempt = %d, want %d", attempt, messageID)
	}
	if result := awaitRunnerPoolPollResult(t, harness); !errors.Is(result.err, wantErr) {
		t.Fatalf("lifecycle acknowledgement error = %v, want %v", result.err, wantErr)
	}
	assertDurableEffect()
	harness.completeMaintenanceCycle(harness.source)
	session.messages <- message
	harness.awaitMessageAcknowledgement(session, messageID)
	harness.completeMaintenanceCycle(harness.source)
}

func deliverIgnoredGitHubHistory(
	t *testing.T,
	harness *e2eHarness,
	session *e2eGitHubSession,
) {
	t.Helper()
	drainAcquisitionIDs(session)
	message := &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobAssignedMessages: []*upstream.JobAssigned{
			nil,
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2001}},
			{},
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: -1}},
		},
		JobStartedMessages: []*upstream.JobStarted{
			nil,
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: -1}},
			{},
		},
		JobCompletedMessages: []*upstream.JobCompleted{
			nil,
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: -1}},
			{},
		},
	}
	harness.deliver(message)
	select {
	case requested := <-session.acquisitions:
		t.Fatalf("ignored GitHub history triggered acquisition %v", requested)
	default:
	}
	harness.completeMaintenanceCycle(harness.execution)
	if attempts := harness.provider.attemptCount(); attempts != 1 {
		t.Fatalf("ignored GitHub history changed provider attempts to %d, want 1", attempts)
	}
}

func messageMatrixProcessing(t *testing.T, harness *e2eHarness, wantJobID core.JobID) core.ProcessingJob {
	t.Helper()
	processing, err := harness.runtime.state.store.processing.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].Job.ID != wantJobID {
		t.Fatalf("message-matrix processing = %+v, error = %v", processing, err)
	}
	return processing[0]
}

func assertMessageMatrixStarted(t *testing.T, harness *e2eHarness, wantJobID core.JobID) {
	t.Helper()
	processing := messageMatrixProcessing(t, harness, wantJobID)
	if !processing.RunnerAccepted || processing.SourceTerminal {
		t.Fatalf("message-matrix started processing = %+v", processing)
	}
}

func assertMessageMatrixTerminal(t *testing.T, harness *e2eHarness, wantJobID core.JobID) {
	t.Helper()
	processing := messageMatrixProcessing(t, harness, wantJobID)
	if !processing.RunnerAccepted || !processing.SourceTerminal {
		t.Fatalf("message-matrix terminal processing = %+v", processing)
	}
}

func awaitAcquisitionIDs(t *testing.T, session *e2eGitHubSession) []int64 {
	t.Helper()
	select {
	case requestIDs := <-session.acquisitions:
		return requestIDs
	case <-time.After(e2eWaitLimit):
		t.Fatal("timed out waiting for GitHub acquisition IDs")
		return nil
	}
}

func drainAcquisitionIDs(session *e2eGitHubSession) {
	for {
		select {
		case <-session.acquisitions:
		default:
			return
		}
	}
}
