package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func (state *lifecycleClusterState) ApplyMessageUpdate(
	ctx context.Context,
	update scaler.MessageUpdate,
) (scaler.MessageUpdateResult, error) {
	if state.busyErr != nil && len(update.Started) > 0 && !errors.Is(state.busyErr, pgx.ErrNoRows) {
		return scaler.MessageUpdateResult{}, state.busyErr
	}
	result, err := state.store.ApplyMessageUpdate(ctx, update)
	if err != nil {
		return scaler.MessageUpdateResult{}, fmt.Errorf("apply in-memory message update: %w", err)
	}
	return result, nil
}

func TestClusterMessageHandlerMarksRunnerBusyBeforeAcknowledgement(t *testing.T) {
	handler, state := busyClusterMessageFixture(t)
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}, RunnerName: "linux-7", RunnerID: 99,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(lifecycles) != 1 || lifecycles[0].Phase != scaler.RunnerBusy {
		t.Fatalf("lifecycles=%#v", lifecycles)
	}
}

func TestClusterMessageHandlerReportsBusyPersistenceBoundaries(t *testing.T) {
	handler, state := busyClusterMessageFixture(t)
	state.busyErr = pgx.ErrNoRows
	if err := handler.HandleMessage(context.Background(), startedClusterMessage(8)); err != nil {
		t.Fatal(err)
	}
	state.busyErr = errors.New("busy")
	if err := handler.HandleMessage(context.Background(), startedClusterMessage(9)); err == nil {
		t.Fatal("expected busy persistence failure")
	}
}

func startedClusterMessage(requestID int64) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		Statistics:         &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID}}},
	}
}

func busyClusterMessageFixture(t *testing.T) (clusterMessageHandler, *lifecycleClusterState) {
	t.Helper()
	state := newLifecycleClusterState(&fakeClusterState{})
	runner := scaler.RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-7", RequestID: 7, Phase: scaler.RunnerWaitingForJob,
	}
	if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	session := &repositoryScaleSetSession{
		key:          repositoryScaleSetSessionKey{scaleSet: "linux", repository: "repo"},
		assignedJobs: inertAssignedJobRecovery(),
	}
	return clusterMessageHandler{
		store: state, session: session,
		scheduler: &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}},
	}, state
}
