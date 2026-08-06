package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
)

func TestProductionRuntimeBuildAndCloseUseInjectedComposition(t *testing.T) {
	runtime, builds := newTestProductionRuntime(t)
	first, err := runtime.Build(context.Background(), config.Config{})
	if err != nil || first == nil {
		t.Fatalf("first runtime build = (%v, %v)", first, err)
	}
	second, err := runtime.Build(context.Background(), config.Config{})
	if err != nil || second == nil || *builds != 2 {
		t.Fatalf("replacement runtime build = (%v, %v), builds %d", second, err, *builds)
	}
	if err = runtime.Close(context.Background()); err != nil {
		t.Fatalf("close production runtime: %v", err)
	}
}

func TestProductionRuntimeReusesMemberIdentityAcrossReload(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	runtime.state.member = daemonTestID(7)
	members := make([]core.MemberID, 0, 2)
	runtime.build = func(
		_ context.Context,
		_ config.Config,
		state *productionState,
		_ *localLeadershipGate,
		_ func(error),
	) (*maintenanceGeneration, error) {
		members = append(members, state.member)
		return newMaintenanceGeneration(
			&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, func(error) {}, nil,
			nil,
		), nil
	}

	if _, err := runtime.Build(context.Background(), config.Config{}); err != nil {
		t.Fatalf("initial build: %v", err)
	}
	if _, err := runtime.Build(context.Background(), config.Config{}); err != nil {
		t.Fatalf("reload build: %v", err)
	}
	if len(members) != 2 || members[0] != daemonTestID(7) || members[1] != daemonTestID(7) {
		t.Fatalf("reload member identities = %v", members)
	}
}

func TestProductionRuntimeCloseWithdrawsMember(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("member withdrawal failed")
	store := &shutdownStoreFake{builderStoreFake: newBuilderStoreFake(), withdrawErr: wantErr}
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	runtime := &productionRuntime{logging: logging, state: &productionState{
		store: store, member: daemonTestID(7),
	}}

	if err := runtime.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v, want transition failure", err)
	}
	if len(store.withdrawn) != 1 || store.withdrawn[0] != daemonTestID(7) {
		t.Fatalf("withdrawn member capacities = %#v, want member 7", store.withdrawn)
	}
}

func newTestProductionRuntime(t *testing.T) (*productionRuntime, *int) {
	t.Helper()
	builds := 0
	buildGeneration := func(
		context.Context,
		config.Config,
		*productionState,
		*localLeadershipGate,
		func(error),
	) (*maintenanceGeneration, error) {
		builds++
		return newMaintenanceGeneration(
			&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, func(error) {}, nil,
			nil,
		), nil
	}
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	return &productionRuntime{
		logging: logging, state: &productionState{store: newBuilderStoreFake(), member: daemonTestID(1)}, processGate: &localLeadershipGate{}, report: func(error) {}, firstBuild: true,
		build: buildGeneration, reporter: &productionRuntimeReporter{},
	}, &builds
}

type shutdownStoreFake struct {
	*builderStoreFake
	withdrawn   []core.MemberID
	withdrawErr error
}

func (store *shutdownStoreFake) WithdrawMemberCapacity(_ context.Context, member core.MemberID) error {
	store.withdrawn = append(store.withdrawn, member)
	return store.withdrawErr
}

func TestRunUsesInjectedApplicationBoundaries(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	dependencies, loads := testRunBoundaries(t, runtime)
	application := NewApplication()
	application.dependencies = dependencies
	if err := application.Run(context.Background(), Options{ConfigPath: "config.yml"}); err != nil || *loads != 2 {
		t.Fatalf("Run = %v, loads %d", err, *loads)
	}
}

func TestRunRejectsInvalidActiveRunnerPolicyBeforeLoadingConfiguration(t *testing.T) {
	loaded := false
	dependencies := daemonApplicationDependencies{loadConfig: func(string, config.ReadFile) (config.Config, error) {
		loaded = true
		return config.Config{}, nil
	}}

	if err := runDaemon(context.Background(), Options{ActiveRunnerPolicy: "destroy"}, dependencies); err == nil {
		t.Fatal("Run accepted an invalid active runner policy")
	}
	if loaded {
		t.Fatal("Run loaded configuration before validating active runner policy")
	}
}

func TestRunReportsConfigLoadFailure(t *testing.T) {
	wantErr := errors.New("application failed")
	dependencies := daemonApplicationDependencies{loadConfig: func(string, config.ReadFile) (config.Config, error) {
		return config.Config{}, wantErr
	}}
	if err := runDaemon(context.Background(), Options{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("load error = %v", err)
	}
}

func TestRunReportsRuntimeOpenFailure(t *testing.T) {
	wantErr := errors.New("application failed")
	dependencies := daemonApplicationDependencies{
		loadConfig: func(string, config.ReadFile) (config.Config, error) { return config.Config{}, nil },
		openRuntime: func(context.Context, config.Config, productionDependencies) (*productionRuntime, error) {
			return nil, wantErr
		},
	}
	if err := runDaemon(context.Background(), Options{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("open error = %v", err)
	}
}

func TestRunReportsSupervisorFailure(t *testing.T) {
	wantErr := errors.New("application failed")
	runtime, _ := newTestProductionRuntime(t)
	dependencies, _ := testRunBoundaries(t, runtime)
	dependencies.supervise = func(
		context.Context, <-chan struct{}, func() (config.Config, error), generationBuilder, serviceNotifier, func(error),
		ActiveRunnerPolicy,
	) error {
		return wantErr
	}
	if err := runDaemon(context.Background(), Options{}, dependencies); !errors.Is(err, wantErr) {
		t.Fatalf("supervise error = %v", err)
	}
}

func testRunBoundaries(t *testing.T, runtime *productionRuntime) (daemonApplicationDependencies, *int) {
	t.Helper()
	loads := 0
	dependencies := daemonApplicationDependencies{}
	dependencies.loadConfig = func(string, config.ReadFile) (config.Config, error) { loads++; return config.Config{}, nil }
	dependencies.openRuntime = func(context.Context, config.Config, productionDependencies) (*productionRuntime, error) {
		return runtime, nil
	}
	dependencies.supervise = func(
		ctx context.Context,
		_ <-chan struct{},
		load func() (config.Config, error),
		build generationBuilder,
		_ serviceNotifier,
		_ func(error),
		policy ActiveRunnerPolicy,
	) error {
		if policy != LeaveActiveRunners {
			t.Fatalf("default active runner policy = %q", policy)
		}
		configured, loadErr := load()
		if loadErr != nil {
			return loadErr
		}
		if _, loadErr = load(); loadErr != nil {
			return loadErr
		}
		_, buildErr := build(ctx, configured)
		return buildErr
	}
	return dependencies, &loads
}
