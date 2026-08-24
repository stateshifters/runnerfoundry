//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestProductContractPostgresReconnectPreservesAcceptedProcessing(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	state, started := createAcceptedPostgresProcessing(t)
	completeAcceptedPostgresProcessingAfterReconnect(t, state, started)
}

func createAcceptedPostgresProcessing(t *testing.T) (config.StateConfig, execution.StartRequest) {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}
	first := newE2EHarness(t, state, "e2e-reconnect-member", false)
	first.establishSourceSession()
	first.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 601, QueueTime: first.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	started := awaitProviderStart(t, "pre-reconnect provider start", first.provider.starts)
	first.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 601}}},
	})
	first.close()
	return state, started
}

func completeAcceptedPostgresProcessingAfterReconnect(
	t *testing.T,
	state config.StateConfig,
	started execution.StartRequest,
) {
	t.Helper()
	second := newE2EHarness(t, state, "e2e-reconnect-member", false)
	second.establishSourceSession()
	processing, err := second.runtime.state.store.ProcessingByMember(
		context.Background(), second.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 1 || processing[0].Job.ID != started.JobID || !processing[0].RunnerStarted {
		t.Fatalf("accepted processing after PostgreSQL reconnect = %s, error = %v", describeProcessing(processing), err)
	}
	second.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: string(started.RunnerName), Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 601}}},
	})
	second.completeMaintenanceCycle(second.execution)
	processing, err = second.runtime.state.store.ProcessingByMember(
		context.Background(), second.runtime.state.member, "linux",
	)
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing after post-reconnect terminal completion = %s, error = %v", describeProcessing(processing), err)
	}
}

func TestProductContractPostgresNotificationWakesDurableQueue(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	harness := newPostgresE2EHarness(t)
	harness.establishSourceSession()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 701, QueueTime: harness.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	awaitProviderStart(t, "notification-driven provider start", harness.provider.starts)
}

func TestProductContractPostgresMaintenanceRecoversMissedNotification(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	harness := newE2EHarness(t, config.StateConfig{Postgres: &settings}, "e2e-missed-notification", true)
	harness.establishSourceSession()
	harness.provider.setInspectionError(errors.New("scripted inventory outage"))
	harness.completeMaintenanceCycle(harness.execution)
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 702, QueueTime: harness.currentTime.AddDate(-10, 0, 0),
		}}},
	})
	assertExactDemandState(t, harness, 702, core.QueueAlreadyQueued)
	if attempts := harness.provider.attemptCount(); attempts != 0 {
		t.Fatalf("provider attempts while notification is missed and inventory unavailable = %d, want 0", attempts)
	}
	harness.provider.setInspectionError(nil)
	harness.completeMaintenanceCycle(harness.execution)
	awaitProviderStart(t, "maintenance-recovered provider start", harness.provider.starts)
}
