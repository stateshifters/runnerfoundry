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

func TestClusteredExecutorRetainsReapUntilSecondInspectionConfirmsAbsence(t *testing.T) {
	configured := clusteredConfig(t)
	configured.ScaleSets[0].Docker = nil
	configured.ScaleSets[0].Libvirt = &config.LibvirtConfig{ConnectionURI: "test", DomainXMLTemplate: "test"}
	runner := cluster.LibvirtRunnerLifecycle{
		NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 2, RunnerName: "linux-2",
	}
	state := &fakeClusterState{pendingLibvirtReaps: []cluster.LibvirtRunnerLifecycle{runner}}
	instance := scaler.RunnerInstance{ID: "domain", RunnerName: runner.RunnerName, State: scaler.RunnerStateRunning}
	provider := &clusterReapingProvider{lists: [][]scaler.RunnerInstance{{instance}, {instance}, nil}}
	daemon := clusteredDaemonForTest(state)
	daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "runner-host-1", configured, slog.Default(), state
	daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: provider}}, nil
	}

	daemon.executeAssignments(context.Background())
	if len(state.confirmedLibvirtReaps) != 0 || provider.destroyed != 1 {
		t.Fatalf("first tick confirmations=%#v destroys=%d", state.confirmedLibvirtReaps, provider.destroyed)
	}
	daemon.executeAssignments(context.Background())
	if len(state.confirmedLibvirtReaps) != 1 || state.confirmedLibvirtReaps[0] != runner {
		t.Fatalf("second tick confirmations=%#v", state.confirmedLibvirtReaps)
	}
}

func TestClusteredExecutorRetainsReapWhenLocalLibvirtConfigurationIsMissing(t *testing.T) {
	configured := clusteredConfig(t)
	runner := cluster.LibvirtRunnerLifecycle{
		NodeID: "runner-host-1", ScaleSet: "missing", Repository: "repo", RequestID: 2, RunnerName: "missing-2",
	}
	state := &fakeClusterState{pendingLibvirtReaps: []cluster.LibvirtRunnerLifecycle{runner}}
	daemon := clusteredDaemonForTest(state)
	daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "runner-host-1", configured, slog.Default(), state

	daemon.executeAssignments(context.Background())
	if len(state.confirmedLibvirtReaps) != 0 {
		t.Fatalf("missing configuration confirmed reaps %#v", state.confirmedLibvirtReaps)
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

func TestClusteredExecutorRetainsReapsOnBoundaryFailures(t *testing.T) {
	runClusterReapBoundaryFailure(t, "load", func(_ *clusteredDaemon, state *fakeClusterState) {
		state.pendingLibvirtReapsErr = errors.New("load failed")
	})
	runClusterReapBoundaryFailure(t, "build", func(daemon *clusteredDaemon, _ *fakeClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{}, errors.New("build failed")
		}
	})
	runClusterReapBoundaryFailure(t, "reap", func(daemon *clusteredDaemon, _ *fakeClusterState) {
		daemon.buildReconciler = failingClusterReaperBuilder
	})
	runClusterReapBoundaryFailure(t, "confirm", func(daemon *clusteredDaemon, state *fakeClusterState) {
		state.confirmLibvirtReapErr = errors.New("confirm failed")
		daemon.buildReconciler = absentClusterReaperBuilder
	})
}

func runClusterReapBoundaryFailure(
	t *testing.T,
	name string,
	configure func(*clusteredDaemon, *fakeClusterState),
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		runner := cluster.LibvirtRunnerLifecycle{NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 2, RunnerName: "linux-2"}
		state := &fakeClusterState{pendingLibvirtReaps: []cluster.LibvirtRunnerLifecycle{runner}}
		daemon := clusteredDaemonForTest(state)
		configured := clusteredConfig(t)
		configured.ScaleSets[0].Docker = nil
		configured.ScaleSets[0].Libvirt = &config.LibvirtConfig{}
		daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "runner-host-1", configured, slog.Default(), state
		configure(daemon, state)
		daemon.executeAssignments(context.Background())
	})
}

func failingClusterReaperBuilder(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
	return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{listErr: errors.New("list failed")}}}, nil
}

func absentClusterReaperBuilder(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
	return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{}}}, nil
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
	daemonUnderTest.assignments = state
	daemonUnderTest.tryLeader = fakeTryLeader(state)
	daemonUnderTest.waitForLeader = fakeWaitForLeader(state)
	return daemonUnderTest, state, provider
}
