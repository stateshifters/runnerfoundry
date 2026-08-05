package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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
func (state *lifecycleClusterState) MarkRunnerBusy(ctx context.Context, scaleSet, repository string, identity scaler.RunnerIdentity, githubRunnerID int64, startedAt time.Time) error {
	if state.busyErr != nil {
		return state.busyErr
	}
	if err := state.store.MarkRunnerBusy(ctx, scaleSet, repository, identity, githubRunnerID, startedAt); err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) BindRunnerIdentity(ctx context.Context, scaleSet, repository string, identity scaler.RunnerIdentity, githubRunnerID int64, startedAt time.Time) error {
	if err := state.store.BindRunnerIdentity(ctx, scaleSet, repository, identity, githubRunnerID, startedAt); err != nil {
		return fmt.Errorf("bind runner identity: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) RequestRunnerReap(ctx context.Context, scaleSet, repository string, identity scaler.RunnerIdentity, reason string, requestedAt time.Time) error {
	if err := state.store.RequestRunnerReap(ctx, scaleSet, repository, identity, reason, requestedAt); err != nil {
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
func TestAssignmentExecutionPhasesRetainStructuredLogValues(t *testing.T) {
	if assignmentExecutionPhaseStart != "start" {
		t.Fatalf("start phase = %q, want start", assignmentExecutionPhaseStart)
	}
	if assignmentExecutionPhaseRepair != "repair" {
		t.Fatalf("repair phase = %q, want repair", assignmentExecutionPhaseRepair)
	}
}

func TestClusteredRunnerReaperDefersBusyRegistrationWithoutError(t *testing.T) {
	runner := scaler.RunnerLifecycle{ScaleSet: "linux", Repository: "repo", RunnerName: "linux-1",
		GitHubRunnerID: 7, Phase: scaler.RunnerReapPending}
	registry := &busyRunnerRegistry{runner: scaler.JITRunner{ID: 7, Name: "linux-1"}}
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: registry,
	}}
	logs := &bytes.Buffer{}
	state := newLifecycleClusterState(&fakeClusterState{})
	now := time.Date(2026, time.July, 31, 12, 0, 0, 0, time.UTC)
	daemon := clusteredDaemon{
		logger:            slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		runnerReapRetries: newRunnerReapRetryTracker(func() time.Time { return now }),
	}

	daemon.reapPendingRunnerLifecycle(context.Background(), state, reconciler, runner)
	daemon.reapPendingRunnerLifecycle(context.Background(), state, reconciler, runner)

	if registry.removeCalls != 1 || len(state.confirmed) != 0 {
		t.Fatalf("remove calls=%d confirmed=%#v", registry.removeCalls, state.confirmed)
	}
	if output := logs.String(); !strings.Contains(output, "action=runner_reap phase=deferred") || strings.Contains(output, "level=ERROR") {
		t.Fatalf("unexpected logs: %s", output)
	}
}

func TestClusteredRunnerReaperReportsUnexpectedFailure(t *testing.T) {
	runner := scaler.RunnerLifecycle{ScaleSet: "linux", Repository: "repo", RunnerName: "linux-1",
		GitHubRunnerID: 7, Phase: scaler.RunnerReapPending}
	registry := &lifecycleRegistryFailure{runner: scaler.JITRunner{ID: 7, Name: "linux-1"}}
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: registry,
	}}
	logs := &bytes.Buffer{}
	daemon := clusteredDaemon{logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}

	daemon.reapPendingRunnerLifecycle(context.Background(), newLifecycleClusterState(&fakeClusterState{}), reconciler, runner)

	if output := logs.String(); !strings.Contains(output, "action=runner_reap phase=failed") ||
		!strings.Contains(output, "level=ERROR") || strings.Contains(output, "phase=deferred") {
		t.Fatalf("unexpected logs: %s", output)
	}
}

type lifecycleRegistryFailure struct {
	runner scaler.JITRunner
}

func (*lifecycleRegistryFailure) Generate(context.Context, string) (scaler.JITRunnerConfig, error) {
	return scaler.JITRunnerConfig{}, errors.New("unexpected generate")
}

func (*lifecycleRegistryFailure) Remove(context.Context, scaler.JITRunner) error {
	return errors.New("github unavailable")
}

func (registry *lifecycleRegistryFailure) Find(context.Context, string) (scaler.JITRunner, bool, error) {
	return registry.runner, true, nil
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
		nodeID: "runner-host-1",
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
	if len(state.publications) != 1 || state.publications[0].nodeID != "runner-host-1" ||
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
		if nodeID != "runner-host-1" {
			t.Fatalf("assignment was claimed for node %q", nodeID)
		}
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
