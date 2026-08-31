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
)

func TestProductContractCleanupFailureRetainsTerminalProcessingSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseCleanupFailureRecovery(t, newSQLiteE2EHarness(t))
}

func TestProductContractPartialProvisioningCleanupGatesRetrySQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exercisePartialProvisioningCleanupGatesRetry(t, newSQLiteE2EHarness(t))
}

func exercisePartialProvisioningCleanupGatesRetry(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	failed := failPartialProvisioningAndCleanup(t, harness)
	assertUnknownPartialResourceRetained(t, harness, failed.JobID)
	recoverPartialResourceAndRetry(t, harness, failed.JobID)
}

func failPartialProvisioningAndCleanup(t *testing.T, harness *e2eHarness) execution.StartRequest {
	t.Helper()
	harness.provider.failNextStartWithPartial(errors.New("scripted partial provisioning failure"))
	harness.provider.setDestroyError(errors.New("scripted partial cleanup failure"))
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 311, QueueTime: harness.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	failed := awaitProviderStart(t, "partial provider start failure", harness.provider.attempts)
	awaitRunnerInstance(t, "partial resource cleanup failure", harness.provider.destroyAttempts)
	assertRetainedUnstartedProcessing(t, harness, failed.JobID)
	return failed
}

func assertUnknownPartialResourceRetained(t *testing.T, harness *e2eHarness, jobID core.JobID) {
	t.Helper()
	harness.provider.setDestroyError(nil)
	harness.provider.setInspectionError(errors.New("scripted endpoint inspection failure"))
	harness.completeMaintenanceCycle(harness.execution)
	assertRetainedUnstartedProcessing(t, harness, jobID)
	if attempts := harness.provider.attemptCount(); attempts != 1 {
		t.Fatalf("provider attempts while endpoint state is unknown = %d, want 1", attempts)
	}
}

func recoverPartialResourceAndRetry(t *testing.T, harness *e2eHarness, jobID core.JobID) {
	t.Helper()
	harness.provider.setInspectionError(nil)
	harness.advanceClock(6 * time.Minute)
	drainSignals(harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	harness.await("recovered endpoint inspection", harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "recovered partial resource cleanup", harness.provider.destroys)
	harness.completeMaintenanceCycle(harness.execution)
	retried := awaitProviderStart(t, "retry after confirmed partial cleanup", harness.provider.attempts)
	if retried.JobID != jobID {
		t.Fatalf("retried partial job ID = %s, want original %s", retried.JobID, jobID)
	}
}

func assertRetainedUnstartedProcessing(t *testing.T, harness *e2eHarness, wantJobID core.JobID) {
	t.Helper()
	records, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(records) != 1 || records[0].Job.ID != wantJobID || records[0].RunnerAccepted {
		t.Fatalf("retained unstarted processing = %+v, error = %v, want job %s", records, err, wantJobID)
	}
}

func exerciseCleanupFailureRecovery(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	queueTime := harness.currentTime.AddDate(-10, 0, 0)
	started := startCleanupCandidate(t, harness, queueTime)
	failCleanupAndAssertRetained(t, harness, started)
	recoverCleanupAndAssertRemoved(t, harness)
}

func startCleanupCandidate(t *testing.T, harness *e2eHarness, queueTime time.Time) execution.StartRequest {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 301, QueueTime: queueTime,
		}}},
	})
	return awaitProviderStart(t, "provider start", harness.provider.starts)
}

func failCleanupAndAssertRetained(t *testing.T, harness *e2eHarness, started execution.StartRequest) {
	t.Helper()
	harness.provider.setDestroyError(errors.New("scripted cleanup failure"))
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: string(started.RunnerName), Result: "failed",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 301},
		}},
	})
	drainSignals(harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "failed cleanup attempt", harness.provider.destroyAttempts)
	harness.await("failed-cleanup inventory inspection", harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	harness.await("repeated failed-cleanup inventory inspection", harness.provider.inspected)
	awaitRunnerInstance(t, "repeated failed cleanup attempt", harness.provider.destroyAttempts)

	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 1 || !processing[0].SourceTerminal {
		t.Fatalf("processing after cleanup failure = %s, error = %v", describeProcessing(processing), err)
	}
}

func recoverCleanupAndAssertRemoved(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.provider.setDestroyError(nil)
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "recovered terminal cleanup", harness.provider.destroys)
	harness.await("recovered-cleanup inventory inspection", harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	harness.await("post-recovery execution cycle", harness.provider.inspected)
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing after cleanup recovery = %s, error = %v", describeProcessing(processing), err)
	}
}

func TestProductContractQueuedTerminalDemandIsNeverProvisionedSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseQueuedTerminalDemand(t, newSQLiteE2EHarness(t))
}

func exerciseQueuedTerminalDemand(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	queueTime := harness.currentTime.AddDate(-10, 0, 0)
	active := startCapacityOccupier(t, harness, queueTime)
	terminalizeQueuedDemand(t, harness, queueTime)
	releaseCapacityAndAssertNoStart(t, harness, active)
}

func startCapacityOccupier(t *testing.T, harness *e2eHarness, queueTime time.Time) execution.StartRequest {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 401, QueueTime: queueTime,
		}}},
	})
	active := awaitProviderStart(t, "capacity-occupying provider attempt", harness.provider.attempts)
	awaitProviderStart(t, "capacity-occupying provider start", harness.provider.starts)
	return active
}

func terminalizeQueuedDemand(t *testing.T, harness *e2eHarness, queueTime time.Time) {
	t.Helper()
	before := harness.provider.attemptCount()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(2),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 402, QueueTime: queueTime.Add(1),
		}}},
	})
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobCompletedMessages: []*upstream.JobCompleted{{
			Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 402},
		}},
	})
	harness.completeMaintenanceCycle(harness.execution)
	assertExactDemandState(t, harness, 402, core.QueueSourceTerminal)
	if after := harness.provider.attemptCount(); after != before {
		t.Fatalf("provider attempts after queued terminal signal = %d, want unchanged %d", after, before)
	}
}

func releaseCapacityAndAssertNoStart(t *testing.T, harness *e2eHarness, active execution.StartRequest) {
	t.Helper()
	before := harness.provider.attemptCount()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: string(active.RunnerName), Result: "canceled",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 401},
		}},
	})
	drainSignals(harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "active runner cleanup", harness.provider.destroys)
	harness.await("active cleanup inventory inspection", harness.provider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	harness.await("capacity-restored execution cycle", harness.provider.inspected)
	assertExactDemandState(t, harness, 402, core.QueueSourceTerminal)
	if after := harness.provider.attemptCount(); after != before {
		t.Fatalf("provider attempts after capacity restoration = %d, want unchanged %d", after, before)
	}
}

func TestProductContractGracefulShutdownJoinsSourceAndMaintenanceSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseGracefulShutdown(t, newSQLiteE2EHarness(t))
}

func exerciseGracefulShutdown(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()

	harness.close()
	for runnerPool, session := range harness.host.sessions {
		harness.await("GitHub session closure for "+runnerPool, session.closed)
	}
	harness.await("membership ticker stop", harness.membership.stopped)
	harness.await("source ticker stop", harness.source.stopped)
	harness.await("execution ticker stop", harness.execution.stopped)
}
