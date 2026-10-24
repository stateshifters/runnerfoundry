//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestProductContractRunnerPoolIsolationSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseRunnerPoolIsolation(t, newSQLiteE2EHarness(t))
}

func TestProductContractRunnerPoolIsolationPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseRunnerPoolIsolation(t, newPostgresE2EHarness(t))
}

func exerciseRunnerPoolIsolation(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.establishSourceSession()
	queueTime := harness.currentTime.AddDate(-10, 0, 0)
	deliverPoolDemand(harness, "linux", queueTime.Add(time.Second), 601, 602)
	deliverPoolDemand(harness, "windows", queueTime, 701, 702)

	linuxFirst := awaitProviderStart(t, "first Linux provider start", harness.provider.starts)
	windowsFirst := awaitProviderStart(t, "first Windows provider start", harness.windowsProvider.starts)
	assertPoolStart(t, harness, linuxFirst, "linux", 601, harness.provider.EndpointID())
	assertPoolStart(t, harness, windowsFirst, "windows", 701, harness.windowsProvider.EndpointID())

	terminalizePoolRequest(harness, "linux", linuxFirst.RunnerName, 601)
	terminalizePoolRequest(harness, "windows", windowsFirst.RunnerName, 701)
	completeCrossPoolCleanup(t, harness)

	linuxSecond := awaitProviderStart(t, "second Linux provider start", harness.provider.starts)
	windowsSecond := awaitProviderStart(t, "second Windows provider start", harness.windowsProvider.starts)
	assertPoolStart(t, harness, linuxSecond, "linux", 602, harness.provider.EndpointID())
	assertPoolStart(t, harness, windowsSecond, "windows", 702, harness.windowsProvider.EndpointID())
}

func deliverPoolDemand(harness *e2eHarness, runnerPool string, queuedAt time.Time, requestIDs ...int64) {
	available := make([]*upstream.JobAvailable, 0, len(requestIDs))
	for index, requestID := range requestIDs {
		available = append(available, &upstream.JobAvailable{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID,
			QueueTime:       queuedAt.Add(time.Duration(index) * time.Second),
		}})
	}
	harness.deliverTo(runnerPool, &upstream.RunnerScaleSetMessage{
		Statistics:           e2eMessageStatistics(len(requestIDs)),
		JobAvailableMessages: available,
	})
}

func terminalizePoolRequest(harness *e2eHarness, runnerPool string, runnerName core.RunnerName, requestID int64) {
	harness.deliverTo(runnerPool, &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: string(runnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	})
}

func completeCrossPoolCleanup(t *testing.T, harness *e2eHarness) {
	t.Helper()
	drainSignals(harness.provider.inspected)
	drainSignals(harness.windowsProvider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "Linux terminal cleanup", harness.provider.destroys)
	awaitRunnerInstance(t, "Windows terminal cleanup", harness.windowsProvider.destroys)
	harness.await("Linux cleanup inventory inspection", harness.provider.inspected)
	harness.await("Windows cleanup inventory inspection", harness.windowsProvider.inspected)
	harness.completeMaintenanceCycle(harness.execution)
	harness.await("Linux post-cleanup inventory inspection", harness.provider.inspected)
	harness.await("Windows post-cleanup inventory inspection", harness.windowsProvider.inspected)
}

func assertPoolStart(
	t *testing.T,
	harness *e2eHarness,
	started execution.StartRequest,
	runnerPool core.RunnerPool,
	wantRequestID int64,
	wantEndpoint execution.InventoryEndpointID,
) {
	t.Helper()
	if started.RunnerPool != runnerPool || started.EndpointID != wantEndpoint {
		t.Fatalf("provider start = %+v, want pool %q endpoint %q", started, runnerPool, wantEndpoint)
	}
	records, err := harness.runtime.state.store.processing.ProcessingByMember(
		context.Background(), harness.runtime.state.member, runnerPool,
	)
	if err != nil {
		t.Fatalf("load %s processing: %v", runnerPool, err)
	}
	for _, record := range records {
		if record.Job.ID != started.JobID {
			continue
		}
		identity, decodeErr := githubsource.DecodeSourceID(record.Job.Source.ID)
		if decodeErr != nil || identity.RequestID != wantRequestID {
			t.Fatalf("%s source identity = (%+v, %v), want request %d", runnerPool, identity, decodeErr, wantRequestID)
		}
		return
	}
	t.Fatalf("started job %s not found in %s processing: %+v", started.JobID, runnerPool, records)
}
