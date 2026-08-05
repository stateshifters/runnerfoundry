package daemon

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusteredExecutorProcessesProviderIndependentReaps(t *testing.T) {
	configured := clusteredConfig(t)
	runner := scaler.RunnerLifecycle{NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-2", InstallationID: 3, GitHubScaleSetID: 4, Phase: scaler.RunnerReapPending}
	base := &fakeClusterState{}
	state := newLifecycleClusterState(base)
	state.pending = []scaler.RunnerLifecycle{runner}
	if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	daemon := clusteredDaemonForTest(base)
	daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "runner-host-1", configured, slog.Default(), state
	daemon.credentials = testAppCredentials(t)
	daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry()}}, nil
	}
	if !daemon.executePendingRunnerReaps(context.Background()) || len(state.confirmed) != 1 {
		t.Fatalf("confirmed=%#v", state.confirmed)
	}
}

var clusteredRunnerReapBoundaryCases = []struct {
	name      string
	configure func(*clusteredDaemon, *lifecycleClusterState)
	want      bool
}{
	{name: "invalid timeout", want: false, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.config.ScaleSets[0].JobStartTimeout = "invalid"
	}},
	{name: "expire", want: false, configure: func(_ *clusteredDaemon, state *lifecycleClusterState) { state.expireErr = errors.New("expire") }},
	{name: "load", want: false, configure: func(_ *clusteredDaemon, state *lifecycleClusterState) { state.pendingErr = errors.New("load") }},
	{name: "missing config", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) { daemon.config.ScaleSets[0].Name = "different" }},
	{name: "client", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return nil, errors.New("client")
		}
	}},
	{name: "build", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{}, errors.New("build")
		}
	}},
	{name: "reap", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{listErr: errors.New("list")}, JITConfig: newIncidentJITRegistry()}}, nil
		}
	}},
	{name: "incomplete", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			instance := scaler.RunnerInstance{ID: "id", RunnerName: "linux-2"}
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{lists: [][]scaler.RunnerInstance{{instance}, {instance}}}, JITConfig: newIncidentJITRegistry()}}, nil
		}
	}},
	{name: "confirm", want: true, configure: func(daemon *clusteredDaemon, state *lifecycleClusterState) {
		state.confirmErr = errors.New("confirm")
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry()}}, nil
		}
	}},
}

func TestClusteredExecutorRetainsProviderIndependentReapsAtBoundaries(t *testing.T) {
	runner := scaler.RunnerLifecycle{NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-2", InstallationID: 3, GitHubScaleSetID: 4, Phase: scaler.RunnerReapPending}
	for _, test := range clusteredRunnerReapBoundaryCases {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeClusterState{}
			state := newLifecycleClusterState(base)
			state.pending = []scaler.RunnerLifecycle{runner}
			if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
				t.Fatal(err)
			}
			daemon := clusteredDaemonForTest(base)
			daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "runner-host-1", clusteredConfig(t), slog.Default(), state
			daemon.credentials = testAppCredentials(t)
			test.configure(daemon, state)
			if got := daemon.executePendingRunnerReaps(context.Background()); got != test.want {
				t.Fatalf("result=%t want=%t", got, test.want)
			}
		})
	}
}

type clusterReapingProvider struct {
	lists     [][]scaler.RunnerInstance
	destroyed int
	listErr   error
}

func (provider *clusterReapingProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	if len(provider.lists) == 0 {
		return nil, nil
	}
	instances := provider.lists[0]
	provider.lists = provider.lists[1:]
	return instances, nil
}

func (*clusterReapingProvider) Start(context.Context, scaler.RunnerStartRequest) error { return nil }
func (*clusterReapingProvider) Destroy(context.Context, scaler.RunnerInstance) error   { return nil }
func (provider *clusterReapingProvider) DestroyTerminal(context.Context, scaler.RunnerInstance) error {
	provider.destroyed++
	return nil
}

func clusteredAssignmentExecutorFixture(t *testing.T) (*clusteredDaemon, *fakeClusterState, *fakeScalerProvider) {
	t.Helper()
	configured := clusteredConfig(t)
	state := &fakeClusterState{readyAssignments: []*cluster.Assignment{
		{ID: 1, NodeID: "runner-host-1", ScaleSet: "remote-only", Repository: "repo", RequestID: 1},
		{ID: 2, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 2, InstallationID: 3, GitHubScaleSetID: 4},
	}}
	provider := &fakeScalerProvider{trackStarted: true}
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.buildReconciler = func(
		context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int,
	) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 10, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
	daemonUnderTest.nodeID = "runner-host-1"
	daemonUnderTest.config = configured
	daemonUnderTest.credentials = testAppCredentials(t)
	daemonUnderTest.logger = slog.Default()
	daemonUnderTest.nodes = state
	daemonUnderTest.assignments = newLifecycleClusterState(state)
	daemonUnderTest.tryLeader = fakeTryLeader(state)
	daemonUnderTest.waitForLeader = fakeWaitForLeader(state)
	return daemonUnderTest, state, provider
}
