//go:build e2e

package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestProductContractSQLiteTerminalFinalitySurvivesRestart(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	state := config.StateConfig{Path: filepath.Join(t.TempDir(), "terminal-finality.db")}
	exerciseTerminalFinalityAcrossRestart(t, state, "e2e-sqlite-terminal-restart", 1801)
}

func TestProductContractPostgresTerminalFinalitySurvivesReconnect(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	state := config.StateConfig{Postgres: &settings}
	exerciseTerminalFinalityAcrossRestart(t, state, "e2e-postgres-terminal-reconnect", 1802)
}

func exerciseTerminalFinalityAcrossRestart(
	t *testing.T,
	state config.StateConfig,
	memberName string,
	requestID int64,
) {
	t.Helper()
	first := newE2EHarness(t, state, memberName, false)
	first.establishSourceSession()
	first.deliver(e2eAvailableMessage(first, requestID))
	started := awaitProviderStart(t, "provider start before terminal restart", first.provider.starts)
	terminalizeBeforeInterruptedCleanup(t, first, started, requestID)
	assertTerminalProcessingRetained(t, first, started.JobID)
	first.close()

	second := newE2EHarness(t, state, memberName, false)
	second.establishSourceSession()
	assertTerminalCleanupResumed(t, second)
	redelivery := e2eAvailableMessage(second, requestID)
	redelivery.Statistics = e2eMessageStatistics(0)
	second.deliver(redelivery)
	assertExactDemandState(t, second, requestID, core.QueueSourceTerminal)
	if attempts := second.provider.attemptCount(); attempts != 0 {
		t.Fatalf("provider attempts after terminal redelivery across restart = %d, want 0", attempts)
	}
}

func terminalizeBeforeInterruptedCleanup(
	t *testing.T,
	harness *e2eHarness,
	started execution.StartRequest,
	requestID int64,
) {
	t.Helper()
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobStartedMessages: []*upstream.JobStarted{{
			RunnerName:     string(started.RunnerName),
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	})
	harness.provider.setDestroyError(errors.New("scripted interrupted terminal cleanup"))
	harness.deliver(&upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(0),
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName:     string(started.RunnerName),
			Result:         "succeeded",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	})
	harness.completeMaintenanceCycle(harness.execution)
	awaitRunnerInstance(t, "interrupted terminal cleanup attempt", harness.provider.destroyAttempts)
}

func assertTerminalProcessingRetained(t *testing.T, harness *e2eHarness, wantJobID core.JobID) {
	t.Helper()
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(),
		harness.runtime.state.member,
		"linux",
	)
	if err != nil || len(processing) != 1 || processing[0].Job.ID != wantJobID ||
		!processing[0].RunnerAccepted || !processing[0].SourceTerminal {
		t.Fatalf("terminal processing retained after interrupted cleanup = %s, error = %v",
			describeProcessing(processing), err)
	}
}

func assertTerminalCleanupResumed(t *testing.T, harness *e2eHarness) {
	t.Helper()
	harness.completeMaintenanceCycle(harness.execution)
	processing, err := harness.runtime.state.store.ProcessingByMember(
		context.Background(),
		harness.runtime.state.member,
		"linux",
	)
	if err != nil || len(processing) != 0 {
		t.Fatalf("processing after terminal cleanup resumed = %s, error = %v",
			describeProcessing(processing), err)
	}
	if attempts := harness.provider.attemptCount(); attempts != 0 {
		t.Fatalf("provider attempts while resuming terminal cleanup = %d, want 0", attempts)
	}
}

func e2eAvailableMessage(harness *e2eHarness, requestID int64) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID,
			QueueTime:       harness.currentTime.AddDate(-10, 0, 0),
		}}},
	}
}
