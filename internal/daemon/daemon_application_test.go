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
	buildDaemonGeneration = func(
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
	originalBuild := buildDaemonGeneration
	t.Cleanup(func() { buildDaemonGeneration = originalBuild })
	builds := 0
	buildDaemonGeneration = func(
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
	loads := installRunBoundaries(t, runtime)
	if err := Run(context.Background(), Options{ConfigPath: "config.yml"}); err != nil || *loads != 2 {
		t.Fatalf("Run = %v, loads %d", err, *loads)
	}
}

func TestRunRejectsInvalidActiveRunnerPolicyBeforeLoadingConfiguration(t *testing.T) {
	restoreRunGlobals(t)
	loaded := false
	loadDaemonConfig = func(string) (config.Config, error) {
		loaded = true
		return config.Config{}, nil
	}

	if err := Run(context.Background(), Options{ActiveRunnerPolicy: "destroy"}); err == nil {
		t.Fatal("Run accepted an invalid active runner policy")
	}
	if loaded {
		t.Fatal("Run loaded configuration before validating active runner policy")
	}
}

func TestRunReportsApplicationBoundaryFailures(t *testing.T) {
	wantErr := errors.New("application failed")
	t.Run("load", func(t *testing.T) {
		restoreRunGlobals(t)
		loadDaemonConfig = func(string) (config.Config, error) { return config.Config{}, wantErr }
		if err := Run(context.Background(), Options{}); !errors.Is(err, wantErr) {
			t.Fatalf("load error = %v", err)
		}
	})
	t.Run("open", func(t *testing.T) {
		restoreRunGlobals(t)
		loadDaemonConfig = func(string) (config.Config, error) { return config.Config{}, nil }
		openDaemonProduction = func(context.Context, config.Config) (*productionRuntime, error) { return nil, wantErr }
		if err := Run(context.Background(), Options{}); !errors.Is(err, wantErr) {
			t.Fatalf("open error = %v", err)
		}
	})
	t.Run("supervise", func(t *testing.T) {
		runtime, _ := newTestProductionRuntime(t)
		installRunBoundaries(t, runtime)
		superviseDaemonRuntime = func(
			context.Context, <-chan struct{}, func() (config.Config, error), generationBuilder, serviceNotifier, func(error),
			ActiveRunnerPolicy,
		) error {
			return wantErr
		}
		if err := Run(context.Background(), Options{}); !errors.Is(err, wantErr) {
			t.Fatalf("supervise error = %v", err)
		}
	})
}

func restoreRunGlobals(t *testing.T) {
	t.Helper()
	originalLoad, originalOpen, originalSupervise := loadDaemonConfig, openDaemonProduction, superviseDaemonRuntime
	t.Cleanup(func() {
		loadDaemonConfig, openDaemonProduction, superviseDaemonRuntime = originalLoad, originalOpen, originalSupervise
	})
}

func installRunBoundaries(t *testing.T, runtime *productionRuntime) *int {
	t.Helper()
	originalLoad, originalOpen, originalSupervise := loadDaemonConfig, openDaemonProduction, superviseDaemonRuntime
	t.Cleanup(func() {
		loadDaemonConfig, openDaemonProduction, superviseDaemonRuntime = originalLoad, originalOpen, originalSupervise
	})
	loads := 0
	loadDaemonConfig = func(string) (config.Config, error) { loads++; return config.Config{}, nil }
	openDaemonProduction = func(context.Context, config.Config) (*productionRuntime, error) { return runtime, nil }
	superviseDaemonRuntime = func(
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
	return &loads
}
