package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestAssignmentExecutorStopsAfterClaimFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{claimErr: errors.New("failed")}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	if daemon.executeNextAssignment(context.Background()) {
		t.Fatal("claim failure unexpectedly made progress")
	}
	if !strings.Contains(logs.String(), "claim runner assignment failed") {
		t.Fatalf("claim failure was not logged: %q", logs.String())
	}
}

func TestAssignmentExecutorLeavesRemovedScaleSetFailureInStarting(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{
		readyAssignments: []*cluster.Assignment{{ID: 1, NodeID: "runner-host-1", ScaleSet: "removed"}},
		releaseErr:       errors.New("failed"),
	}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	if daemon.executeNextAssignment(context.Background()) {
		t.Fatal("failed completion unexpectedly made progress")
	}
	if len(state.released) != 1 || state.released[0] != 1 || !strings.Contains(logs.String(), "removed local scale set") {
		t.Fatalf("completion attempts=%v logs=%q", state.released, logs.String())
	}
}

func TestAssignmentExecutorReportsGitHubClientFailure(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("failed")
	}
	err := daemon.executeAssignment(context.Background(), localAssignment())
	if err == nil || !strings.Contains(err.Error(), "create assignment GitHub client") {
		t.Fatalf("expected GitHub client failure, got %v", err)
	}
}

func TestAssignmentExecutorReportsReconcilerFailure(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{}, failure
	}
	if err := daemon.executeAssignment(context.Background(), localAssignment()); !errors.Is(err, failure) {
		t.Fatalf("expected reconciler failure, got %v", err)
	}
}

func TestAssignmentExecutorReportsInitialProviderInspectionFailure(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.buildReconciler = reconcilerWithProvider(&fakeScalerProvider{listErr: errors.New("failed")})
	err := daemon.executeAssignment(context.Background(), localAssignment())
	if err == nil || !strings.Contains(err.Error(), "verify assigned runner") {
		t.Fatalf("expected provider inspection failure, got %v", err)
	}
}

func TestAssignmentExecutorRejectsSuccessfulStartWithoutVisibleRunner(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.buildReconciler = reconcilerWithProvider(&fakeScalerProvider{})
	err := daemon.executeAssignment(context.Background(), localAssignment())
	if err == nil || !strings.Contains(err.Error(), "provider did not start") {
		t.Fatalf("expected missing runner failure, got %v", err)
	}
}

func TestAssignmentExecutorReportsProviderStartFailure(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.buildReconciler = reconcilerWithProvider(failingStartProvider{})
	err := daemon.executeAssignment(context.Background(), localAssignment())
	if err == nil || !strings.Contains(err.Error(), "start assigned runner") {
		t.Fatalf("expected provider start failure, got %v", err)
	}
}

func TestAssignmentExecutorDefersObligationAfterProviderStartFailure(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.buildReconciler = reconcilerWithProvider(failingStartProvider{})
	assignment := localAssignment()
	assignment.State = cluster.AssignmentStarting
	if daemon.executeClaimedAssignment(context.Background(), assignment, assignmentExecutionPhaseStart) {
		t.Fatal("failed provider start unexpectedly made progress")
	}
	if len(state.released) != 1 || state.released[0] != assignment.ID {
		t.Fatalf("deferred assignments = %v", state.released)
	}
}

func TestAssignmentExecutorReportsFailureToDeferFailedStart(t *testing.T) {
	logs := &bytes.Buffer{}
	deferFailure := errors.New("defer unavailable")
	state := &fakeClusterState{releaseErr: deferFailure}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	daemon.buildReconciler = reconcilerWithProvider(failingStartProvider{})
	assignment := localAssignment()
	assignment.State = cluster.AssignmentStarting
	if daemon.executeClaimedAssignment(context.Background(), assignment, assignmentExecutionPhaseStart) {
		t.Fatal("failed provider start unexpectedly made progress")
	}
	output := logs.String()
	if !strings.Contains(output, "start assigned runner") ||
		!strings.Contains(output, "defer failed runner assignment") || !strings.Contains(output, deferFailure.Error()) {
		t.Fatalf("assignment failure log = %q", output)
	}
}

func TestAssignmentExecutorReportsRunningTransitionFailure(t *testing.T) {
	state := &fakeClusterState{startedErr: errors.New("failed")}
	daemon := activeClusteredDaemon(t, state)
	daemon.buildReconciler = reconcilerWithProvider(&fakeScalerProvider{trackStarted: true})
	err := daemon.executeAssignment(context.Background(), localAssignment())
	if err == nil || !strings.Contains(err.Error(), "record started runner assignment") {
		t.Fatalf("expected running transition failure, got %v", err)
	}
}

func localAssignment() *cluster.Assignment {
	return &cluster.Assignment{
		ID: 1, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 2,
		InstallationID: 3, GitHubScaleSetID: 4,
	}
}
