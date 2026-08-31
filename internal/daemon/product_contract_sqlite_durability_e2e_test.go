//go:build e2e

package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestProductContractSQLiteRuntimeRestartContinuesDurableLifecycle(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	databasePath := filepath.Join(t.TempDir(), "runnerfoundry-runtime.db")
	state := config.StateConfig{Path: databasePath}
	first := newE2EHarness(t, state, "e2e-restart-member", false)
	first.establishSourceSession()
	queueTime := first.currentTime.AddDate(-10, 0, 0)
	deliverPoolDemand(first, "linux", queueTime, 801, 802)
	accepted := awaitProviderStart(t, "accepted runner before SQLite restart", first.provider.starts)
	first.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(2),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(accepted.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 801}}},
	})
	first.close()

	second := newE2EHarness(t, state, "e2e-restart-member", false)
	second.establishSourceSession()
	assertRestartedAcceptedProcessing(t, second, accepted.JobID)
	assertExactDemandState(t, second, 802, core.QueueAlreadyQueued)
	second.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(accepted.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 801}}},
	})
	second.completeMaintenanceCycle(second.execution)
	resumed := awaitProviderStart(t, "queued runner after SQLite restart", second.provider.starts)
	assertProcessingRequest(t, second, resumed.JobID, 802)
	completeRestartedLifecycle(t, second, resumed, 802)
}

func TestProductContractSQLiteRestartRetainsUnknownProviderOwnership(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	state, claimed := createUnknownProviderOwnershipCandidate(t)
	assertUnknownProviderOwnershipRetained(t, state, claimed)
}

func createUnknownProviderOwnershipCandidate(t *testing.T) (config.StateConfig, execution.StartRequest) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "runnerfoundry-provider-transition.db")
	state := config.StateConfig{Path: databasePath}
	first := newE2EHarness(t, state, "e2e-provider-transition-member", false)
	first.establishSourceSession()
	first.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 803, QueueTime: first.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	claimed := awaitProviderStart(t, "provider-transition claim", first.provider.starts)
	first.close()
	return state, claimed
}

func assertUnknownProviderOwnershipRetained(
	t *testing.T,
	state config.StateConfig,
	claimed execution.StartRequest,
) {
	t.Helper()
	second := newE2EHarness(t, state, "e2e-provider-transition-member", false)
	second.provider.setEndpointID("e2e:replacement-provider")
	second.advanceClock(5*time.Minute + time.Second)
	second.completeMaintenanceCycle(second.execution)
	processing, err := second.runtime.state.store.ProcessingByMember(
		context.Background(), second.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 1 || processing[0].Job.ID != claimed.JobID ||
		processing[0].InventoryEndpointID != claimed.EndpointID || second.provider.destroyCount() != 0 ||
		second.provider.attemptCount() != 0 {
		t.Fatalf("unknown endpoint recovery state = %s, error = %v, destroys=%d attempts=%d",
			describeProcessing(processing), err, second.provider.destroyCount(), second.provider.attemptCount())
	}
}

func assertRestartedAcceptedProcessing(t *testing.T, harness *e2eHarness, wantJobID core.JobID) {
	t.Helper()
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 1 || processing[0].Job.ID != wantJobID || !processing[0].RunnerAccepted {
		t.Fatalf("accepted processing after runtime restart = %s, error = %v", describeProcessing(processing), err)
	}
}

func completeRestartedLifecycle(
	t *testing.T,
	harness *e2eHarness,
	started execution.StartRequest,
	requestID int64,
) {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(started.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	})
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "restarted lifecycle resource cleanup", harness.provider.destroys)
	harness.completeMaintenanceCycle(harness.execution)
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(), harness.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing after restarted lifecycle completion = %s, error = %v", describeProcessing(processing), err)
	}
}
