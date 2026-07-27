package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type lifecycleClusterState struct {
	*fakeClusterState
	store      *scaler.MemoryDemandStore
	pending    []scaler.RunnerLifecycle
	expireErr  error
	pendingErr error
	confirmErr error
	busyErr    error
	confirmed  []scaler.RunnerLifecycle
}

func newLifecycleClusterState(state *fakeClusterState) *lifecycleClusterState {
	return &lifecycleClusterState{fakeClusterState: state, store: scaler.NewMemoryDemandStore()}
}

func (state *lifecycleClusterState) RegisterRunner(ctx context.Context, runner scaler.RunnerLifecycle) error {
	if err := state.store.RegisterRunner(ctx, runner); err != nil {
		return fmt.Errorf("register runner: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) ConfirmRunnerWaiting(ctx context.Context, runner scaler.RunnerLifecycle, at time.Time) error {
	if err := state.store.ConfirmRunnerWaiting(ctx, runner, at); err != nil {
		return fmt.Errorf("confirm runner waiting: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) MarkRunnerBusy(ctx context.Context, scaleSet, repository string, requestID int64, runnerName string, runnerID int64, at time.Time) error {
	if state.busyErr != nil {
		return state.busyErr
	}
	if err := state.store.MarkRunnerBusy(ctx, scaleSet, repository, requestID, runnerName, runnerID, at); err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) RequestRunnerReap(ctx context.Context, scaleSet, repository string, requestID int64, runnerName, reason string, at time.Time) error {
	if err := state.store.RequestRunnerReap(ctx, scaleSet, repository, requestID, runnerName, reason, at); err != nil {
		return fmt.Errorf("request runner reap: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) RequestExpiredRunnerReaps(ctx context.Context, scaleSet, repository string, deadline, at time.Time) (int, error) {
	if state.expireErr != nil {
		return 0, state.expireErr
	}
	changed, err := state.store.RequestExpiredRunnerReaps(ctx, scaleSet, repository, deadline, at)
	if err != nil {
		return 0, fmt.Errorf("request expired runners: %w", err)
	}
	return changed, nil
}
func (state *lifecycleClusterState) RequestExcessWaitingRunnerReaps(ctx context.Context, scaleSet, repository string, retain int, reason string, at time.Time) (int, error) {
	changed, err := state.store.RequestExcessWaitingRunnerReaps(ctx, scaleSet, repository, retain, reason, at)
	if err != nil {
		return 0, fmt.Errorf("request excess runners: %w", err)
	}
	return changed, nil
}
func (state *lifecycleClusterState) PendingRunnerReaps(ctx context.Context, scaleSet, repository string) ([]scaler.RunnerLifecycle, error) {
	pending, err := state.store.PendingRunnerReaps(ctx, scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("pending runner reaps: %w", err)
	}
	return pending, nil
}
func (state *lifecycleClusterState) PendingNodeRunnerReaps(context.Context, cluster.NodeID) ([]scaler.RunnerLifecycle, error) {
	if state.pendingErr != nil {
		return nil, state.pendingErr
	}
	return append([]scaler.RunnerLifecycle(nil), state.pending...), nil
}
func (state *lifecycleClusterState) ConfirmRunnerReaped(ctx context.Context, runner scaler.RunnerLifecycle) error {
	state.confirmed = append(state.confirmed, runner)
	if state.confirmErr != nil {
		return state.confirmErr
	}
	if err := state.store.ConfirmRunnerReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm runner reaped: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) CompleteJob(ctx context.Context, completion cluster.JobCompletion) error {
	return state.fakeClusterState.CompleteJob(ctx, completion)
}

func TestAssignmentExecutionPhasesRetainStructuredLogValues(t *testing.T) {
	if assignmentExecutionPhaseStart != "start" {
		t.Fatalf("start phase = %q, want start", assignmentExecutionPhaseStart)
	}
	if assignmentExecutionPhaseRepair != "repair" {
		t.Fatalf("repair phase = %q, want repair", assignmentExecutionPhaseRepair)
	}
}

func TestClusteredDaemonPublishesProviderHealth(t *testing.T) {
	configured := clusteredConfig(t)
	configured.ScaleSets = append(configured.ScaleSets, config.ScaleSetConfig{
		Name: "unhealthy", RunnerGroup: "Default", MaxRunners: 2,
		Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	})
	state := &fakeClusterState{}
	daemonFixture := clusteredDaemonForTest(state)
	daemon := clusteredDaemon{
		nodeID: "spock",
		config: configured, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
		loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier,
	}
	daemon.providerHealthy = func(_ context.Context, scaleSet config.ScaleSetConfig) error {
		if scaleSet.Name == "unhealthy" {
			return errors.New("provider unavailable")
		}
		return nil
	}
	if err := daemon.publish(context.Background(), cluster.NodeHealthy); err != nil {
		t.Fatal(err)
	}
	if len(state.publications) != 1 || state.publications[0].nodeID != "spock" ||
		!state.publications[0].contributions[0].Healthy || state.publications[0].contributions[1].Healthy {
		t.Fatalf("provider health was not reflected in contributions: %#v", state.publications)
	}
}

func TestClusteredExecutorUsesOnlyLocalScaleSets(t *testing.T) {
	daemon, state, provider := clusteredAssignmentExecutorFixture(t)
	daemon.executeAssignments(context.Background())
	if len(state.released) != 1 || state.released[0] != 1 {
		t.Fatalf("remote-only assignment was not released: %#v", state.released)
	}
	if len(state.started) != 1 || state.started[0] != 2 || len(provider.started) != 1 {
		t.Fatalf("local assignment was not executed: started=%#v provider=%#v", state.started, provider.started)
	}
	for _, nodeID := range state.claimedNodeIDs {
		if nodeID != "spock" {
			t.Fatalf("assignment was claimed for node %q", nodeID)
		}
	}
}

func TestClusteredExecutorRegistersLibvirtRunnerBeforeStart(t *testing.T) {
	daemon, state, provider := clusteredAssignmentExecutorFixture(t)
	daemon.config.ScaleSets[0].Docker = nil
	daemon.config.ScaleSets[0].Libvirt = &config.LibvirtConfig{ConnectionURI: "test", DomainXMLTemplate: "test"}
	daemon.executeAssignments(context.Background())

	if len(provider.started) != 1 || len(state.registeredLibvirtRunners) != 1 {
		t.Fatalf("started=%#v registered=%#v", provider.started, state.registeredLibvirtRunners)
	}
	registered := state.registeredLibvirtRunners[0]
	if registered.NodeID != "spock" || registered.RequestID != 2 || registered.RunnerName != provider.started[0].RunnerName {
		t.Fatalf("registered lifecycle = %#v; start = %#v", registered, provider.started[0])
	}
}

func TestClusteredExecutorRegistersProviderIndependentLifecycle(t *testing.T) {
	daemon, base, provider := clusteredAssignmentExecutorFixture(t)
	state := newLifecycleClusterState(base)
	daemon.assignments = state
	daemon.executeAssignments(context.Background())
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Provider != scaler.RunnerProviderDocker ||
		lifecycles[0].AssignmentID != 2 || len(provider.started) != 1 {
		t.Fatalf("lifecycles=%#v started=%#v err=%v", lifecycles, provider.started, err)
	}
}

func TestClusteredExecutorRecordsGenericLibvirtProvider(t *testing.T) {
	daemon, base, _ := clusteredAssignmentExecutorFixture(t)
	daemon.config.ScaleSets[0].Docker = nil
	daemon.config.ScaleSets[0].Libvirt = &config.LibvirtConfig{}
	state := newLifecycleClusterState(base)
	daemon.assignments = state
	daemon.executeAssignments(context.Background())
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Provider != scaler.RunnerProviderLibvirt {
		t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
	}
}

func TestClusterMessageHandlerMarksRunnerBusyBeforeAcknowledgement(t *testing.T) {
	handler, state := busyClusterMessageFixture(t)
	if err := handler.removeStarted(context.Background(), []*upstream.JobStarted{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}, RunnerName: "linux-7", RunnerID: 99,
	}}); err != nil {
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
	if err := handler.removeStarted(context.Background(), []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 8}}}); err != nil {
		t.Fatal(err)
	}
	state.busyErr = errors.New("busy")
	if err := handler.removeStarted(context.Background(), []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 9}}}); err == nil {
		t.Fatal("expected busy persistence failure")
	}
}

func busyClusterMessageFixture(t *testing.T) (clusterMessageHandler, *lifecycleClusterState) {
	t.Helper()
	state := newLifecycleClusterState(&fakeClusterState{})
	runner := scaler.RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-7", RequestID: 7, Phase: scaler.RunnerWaitingForJob}
	if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	return clusterMessageHandler{store: state, session: &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{scaleSet: "linux", repository: "repo"},
	}}, state
}
