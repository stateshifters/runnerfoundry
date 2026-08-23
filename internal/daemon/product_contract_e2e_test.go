//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestProductContractAcceptedJobLifecycleSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseAcceptedJobLifecycle(t, newSQLiteE2EHarness(t))
}

func TestProductContractProviderBoundaryDeadlinesSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	harness := newSQLiteE2EHarness(t)
	assertProviderDeadline(t, awaitProviderContext(t, harness.provider, "validate"), 30*time.Second)
	assertEarlierProviderDeadlinePreserved(t, harness)
	if err := harness.providerBoundary.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh scripted provider boundary: %v", err)
	}
	assertProviderDeadline(t, awaitProviderContext(t, harness.provider, "refresh"), 30*time.Second)
	harness.establishSourceSession()
	exerciseAcceptedJobLifecycleWithEstablishedSession(t, harness, 111)
	assertProviderDeadline(t, awaitProviderContext(t, harness.provider, "start"), 5*time.Minute)
	assertProviderDeadline(t, awaitProviderContext(t, harness.provider, "inspect"), 30*time.Second)
}

func assertEarlierProviderDeadlinePreserved(t *testing.T, harness *e2eHarness) {
	t.Helper()
	callerDeadline := time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	if err := harness.providerBoundary.Validate(ctx); err != nil {
		t.Fatalf("validate scripted provider with earlier caller deadline: %v", err)
	}
	observed := awaitProviderContext(t, harness.provider, "validate")
	if !observed.has || !observed.deadline.Equal(callerDeadline) {
		t.Fatalf("provider caller deadline = (%v, %v), want preserved %v", observed.deadline, observed.has, callerDeadline)
	}
}

func assertProviderDeadline(t *testing.T, observed e2eProviderContext, timeout time.Duration) {
	t.Helper()
	remaining := observed.deadline.Sub(observed.observed)
	if !observed.has || remaining > timeout || remaining < timeout-time.Second {
		t.Fatalf("provider %s deadline remaining = %s (present=%v), want within one second of %s",
			observed.operation, remaining, observed.has, timeout)
	}
}

func awaitProviderContext(t *testing.T, provider *e2eProvider, operation string) e2eProviderContext {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		select {
		case observed := <-provider.contexts:
			if observed.operation == operation {
				return observed
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for provider %s context", operation)
			return e2eProviderContext{}
		}
	}
}

func exerciseAcceptedJobLifecycle(t *testing.T, harness *e2eHarness) {
	t.Helper()
	exerciseAcceptedJobLifecycleForRequest(t, harness, 101)
}

func exerciseAcceptedJobLifecycleForRequest(t *testing.T, harness *e2eHarness, requestID int64) {
	t.Helper()
	harness.establishSourceSession()
	exerciseAcceptedJobLifecycleWithEstablishedSession(t, harness, requestID)
}

func exerciseAcceptedJobLifecycleWithEstablishedSession(t *testing.T, harness *e2eHarness, requestID int64) {
	t.Helper()
	assertPublishedCapacity(t, harness.host.sessions["linux"], 1, "initial Linux capacity")
	assertPublishedCapacity(t, harness.host.sessions["windows"], 1, "initial Windows capacity")
	exerciseAcceptedJobLifecycleAfterInitialCapacity(t, harness, requestID)
}

func exerciseAcceptedJobLifecycleAfterInitialCapacity(t *testing.T, harness *e2eHarness, requestID int64) {
	t.Helper()
	queueTime := harness.currentTime.AddDate(-10, 0, 0)
	started := queueAndAssertProviderStart(t, harness, requestID, queueTime)
	markAndAssertRunnerAccepted(t, harness, requestID, started)
	completeAndAssertCleanup(t, harness, requestID, started)
	redeliverAndAssertTerminalIdempotency(t, harness, requestID, queueTime)
}

func queueAndAssertProviderStart(
	t *testing.T,
	harness *e2eHarness,
	requestID int64,
	queueTime time.Time,
) execution.StartRequest {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: queueTime,
		}}},
	})

	started := awaitProviderStart(t, "provider start", harness.provider.starts)
	assertPublishedCapacity(t, harness.host.sessions["linux"], 0, "processing demand")
	if started.RunnerPool != core.RunnerPool("linux") || started.EndpointID != harness.provider.EndpointID() {
		t.Fatalf("provider start = %+v", started)
	}
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 1 || processing[0].Job.ID != started.JobID {
		t.Fatalf("processing after provider start = %s, error = %v", describeProcessing(processing), err)
	}
	return started
}

func markAndAssertRunnerAccepted(
	t *testing.T,
	harness *e2eHarness,
	requestID int64,
	started execution.StartRequest,
) {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{
			RunnerName:     string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	})
	assertPublishedCapacity(t, harness.host.sessions["linux"], 0, "accepted processing demand")
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 1 || !processing[0].RunnerStarted {
		t.Fatalf("processing after runner acceptance = %s, error = %v", describeProcessing(processing), err)
	}
}

func completeAndAssertCleanup(
	t *testing.T,
	harness *e2eHarness,
	requestID int64,
	started execution.StartRequest,
) {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: string(started.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	})
	assertPublishedCapacity(t, harness.host.sessions["linux"], 0, "terminal processing awaiting cleanup")
	drainSignals(harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	destroyed := awaitRunnerInstance(t, "terminal provider cleanup", harness.provider.destroys)
	if destroyed.RunnerName != started.RunnerName {
		t.Fatalf("destroyed runner = %q, want %q", destroyed.RunnerName, started.RunnerName)
	}
	harness.await("terminal cleanup inventory inspection", harness.provider.inspected)
	harness.execution.Tick(harness.currentTime)
	harness.await("post-cleanup execution cycle", harness.provider.inspected)
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing after terminal cleanup = %s, error = %v", describeProcessing(processing), err)
	}
	rotateAndAssertCapacity(t, harness, "linux", 0, 1, "capacity restored after terminal cleanup")
}

func rotateAndAssertCapacity(
	t *testing.T,
	harness *e2eHarness,
	runnerPool string,
	assigned int,
	want int,
	phase string,
) {
	t.Helper()
	harness.deliverTo(runnerPool, &upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(assigned)})
	assertPublishedCapacity(t, harness.host.sessions[runnerPool], want, phase)
}

func assertPublishedCapacity(t *testing.T, session *e2eGitHubSession, want int, phase string) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		observation := session.observeCapacity()
		if observation.tick > observation.read && observation.published == want {
			session.markCapacityRead(observation.tick)
			return
		}
		select {
		case <-observation.changed:
			continue
		case <-timer.C:
			if observation.tick > observation.read {
				t.Fatalf("timed out waiting for %s publication; latest = %d, want %d",
					phase, observation.published, want)
			}
			t.Fatalf("timed out waiting for %s publication", phase)
		}
	}
}

func redeliverAndAssertTerminalIdempotency(
	t *testing.T,
	harness *e2eHarness,
	requestID int64,
	queueTime time.Time,
) {
	t.Helper()
	before := harness.provider.attemptCount()
	session := harness.host.sessions["linux"]
	session.messages <- &upstream.RunnerScaleSetMessage{
		MessageID: 1, Statistics: e2eMessageStatistics(0),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: queueTime,
		}}},
	}
	harness.awaitMessageAcknowledgement(session, 1)
	harness.completeMaintenanceCycle(harness.source)
	harness.completeMaintenanceCycle(harness.execution)
	assertExactDemandState(t, harness, requestID, core.QueueSourceTerminal)
	if after := harness.provider.attemptCount(); after != before {
		t.Fatalf("provider attempts after terminal redelivery = %d, want unchanged %d", after, before)
	}
}

func TestProductContractProvisioningFailureRetriesSameJobFromQueueTailSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseProvisioningFailureRetry(t, newSQLiteE2EHarness(t))
}

func exerciseProvisioningFailureRetry(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	assertPublishedCapacity(t, harness.host.sessions["linux"], 1, "initial retry capacity")
	assertPublishedCapacity(t, harness.host.sessions["windows"], 1, "initial Windows retry capacity")
	firstCleanup := harness.host.blockNextCleanup()
	harness.provider.failNextStart(context.DeadlineExceeded)
	queueTime := harness.currentTime.AddDate(-10, 0, 0)
	queueRetryCandidates(harness, queueTime)
	assertPublishedCapacity(t, harness.host.sessions["linux"], 0, "queued and processing demand")
	failedAttempt := assertGatedFailedAttemptCleanup(t, harness, firstCleanup, 1)
	secondAttempt := assertNextFIFOAttempt(t, harness, failedAttempt)
	assertRepeatedRetryUntilTerminal(t, harness, failedAttempt, secondAttempt)
}

func queueRetryCandidates(harness *e2eHarness, queueTime time.Time) {
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(2),
		JobAvailableMessages: []*upstream.JobAvailable{
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 201, QueueTime: queueTime}},
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 202, QueueTime: queueTime.Add(1)}},
		},
	})
}

func assertGatedFailedAttemptCleanup(
	t *testing.T,
	harness *e2eHarness,
	cleanup *e2eCleanupGate,
	wantAttempts int,
) execution.StartRequest {
	t.Helper()
	failedAttempt := awaitProviderStart(t, "failed provider attempt", harness.provider.attempts)
	harness.await("failed attempt cleanup start", cleanup.started)
	if attempts := harness.provider.attemptCount(); attempts != wantAttempts {
		t.Fatalf("provider attempts before cleanup succeeds = %d, want %d", attempts, wantAttempts)
	}
	close(cleanup.release)
	cleanedBootstrap := awaitString(t, "failed attempt GitHub bootstrap cleanup", harness.host.cleanups)
	if cleanedBootstrap != string(failedAttempt.RunnerName) {
		t.Fatalf("cleaned bootstrap runner = %q, want %q", cleanedBootstrap, failedAttempt.RunnerName)
	}
	return failedAttempt
}

func assertNextFIFOAttempt(
	t *testing.T,
	harness *e2eHarness,
	failedAttempt execution.StartRequest,
) execution.StartRequest {
	t.Helper()
	secondAttempt := awaitProviderStart(t, "next FIFO provider attempt", harness.provider.attempts)
	if secondAttempt.JobID == failedAttempt.JobID {
		t.Fatal("failed job was selected again before the next FIFO job")
	}
	assertProcessingRequest(t, harness, secondAttempt.JobID, 202)
	assertExactDemandState(t, harness, 201, core.QueueAlreadyQueued)
	return secondAttempt
}

func assertRepeatedRetryUntilTerminal(
	t *testing.T,
	harness *e2eHarness,
	failedAttempt execution.StartRequest,
	secondAttempt execution.StartRequest,
) {
	t.Helper()
	firstRetry := harness.provider.blockNextStart(errors.New("scripted repeated provisioning failure one"))
	manualClockAdvance := harness.clockAdvance.Load()
	executionBefore := harness.laneCompleted[harness.execution].count()
	terminalizeSecondRetryCandidate(t, harness, secondAttempt)
	terminalAttempt := exerciseConsecutiveProvisioningFailures(t, harness, firstRetry, failedAttempt.JobID)
	terminalizeBlockedProvisioningAttempt(t, harness, terminalAttempt, failedAttempt.JobID)
	harness.laneCompleted[harness.execution].waitAfter(t, "terminalized retry execution cycle", executionBefore)
	assertExactDemandState(t, harness, 201, core.QueueSourceTerminal)
	if after := harness.provider.attemptCount(); after != 5 {
		t.Fatalf("provider attempts after source terminalization = %d, want 5", after)
	}
	if harness.clockAdvance.Load() != manualClockAdvance {
		t.Fatal("provisioning retries required controlled clock advancement")
	}
}

func terminalizeSecondRetryCandidate(
	t *testing.T,
	harness *e2eHarness,
	secondAttempt execution.StartRequest,
) {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: string(secondAttempt.RunnerName), Result: "canceled",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 202},
		}},
	})
	harness.execution.Tick(harness.currentTime)
	awaitRunnerInstance(t, "second job cleanup", harness.provider.destroys)
	awaitString(t, "second job bootstrap cleanup", harness.host.cleanups)
}

func exerciseConsecutiveProvisioningFailures(
	t *testing.T,
	harness *e2eHarness,
	firstRetry *e2eStartGate,
	wantJobID core.JobID,
) *e2eStartGate {
	t.Helper()
	secondRetry := assertStableGatedRetry(t, harness, firstRetry, wantJobID, 3,
		errors.New("scripted repeated provisioning failure two"))
	return assertStableGatedRetry(t, harness, secondRetry, wantJobID, 4,
		errors.New("scripted terminal-race provisioning failure"))
}

func terminalizeBlockedProvisioningAttempt(
	t *testing.T,
	harness *e2eHarness,
	terminalAttempt *e2eStartGate,
	wantJobID core.JobID,
) {
	t.Helper()
	harness.await("terminal-race provider start", terminalAttempt.started)
	lastAttempt := awaitProviderStart(t, "terminal-race provider attempt", harness.provider.attempts)
	if lastAttempt.JobID != wantJobID {
		t.Fatalf("terminal-race JobID = %s, want stable %s", lastAttempt.JobID, wantJobID)
	}
	terminalizeProvisioningRetry(t, harness)
	terminalCleanup := harness.host.blockNextCleanup()
	close(terminalAttempt.release)
	harness.await("terminal-race cleanup start", terminalCleanup.started)
	if attempts := harness.provider.attemptCount(); attempts != 5 {
		t.Fatalf("provider attempts before terminal cleanup succeeds = %d, want 5", attempts)
	}
	close(terminalCleanup.release)
	awaitString(t, "terminal-race bootstrap cleanup", harness.host.cleanups)
}

func assertStableGatedRetry(
	t *testing.T,
	harness *e2eHarness,
	start *e2eStartGate,
	wantJobID core.JobID,
	wantAttempts int,
	nextFailure error,
) *e2eStartGate {
	t.Helper()
	harness.await("repeated provider start", start.started)
	attempt := awaitProviderStart(t, "repeated provider attempt", harness.provider.attempts)
	if attempt.JobID != wantJobID {
		t.Fatalf("repeated retry JobID = %s, want stable %s", attempt.JobID, wantJobID)
	}
	cleanup := harness.host.blockNextCleanup()
	next := harness.provider.blockNextStart(nextFailure)
	close(start.release)
	harness.await("repeated attempt cleanup start", cleanup.started)
	if attempts := harness.provider.attemptCount(); attempts != wantAttempts {
		t.Fatalf("provider attempts while cleanup is blocked = %d, want %d", attempts, wantAttempts)
	}
	close(cleanup.release)
	awaitString(t, "repeated attempt bootstrap cleanup", harness.host.cleanups)
	return next
}

func terminalizeProvisioningRetry(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{
			Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 201},
		}},
	})
}

func assertProcessingRequest(t *testing.T, harness *e2eHarness, jobID core.JobID, wantRequestID int64) {
	t.Helper()
	records, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil {
		t.Fatalf("load processing jobs: %v", err)
	}
	for _, record := range records {
		if record.Job.ID != jobID {
			continue
		}
		identity, decodeErr := githubsource.DecodeSourceID(record.Job.Source.ID)
		if decodeErr != nil || identity.RequestID != wantRequestID {
			t.Fatalf("processing source identity = (%+v, %v), want request %d", identity, decodeErr, wantRequestID)
		}
		return
	}
	t.Fatalf("processing job %s not found in %s", jobID, describeProcessing(records))
}
