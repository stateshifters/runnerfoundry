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
	) (*unifiedGeneration, error) {
		members = append(members, state.member)
		return newUnifiedGeneration(
			&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, func(error) {}, nil,
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
	) (*unifiedGeneration, error) {
		builds++
		return newUnifiedGeneration(
			&cycleEngineFake{}, &manualCycleTicker{ticks: make(chan time.Time)}, func(error) {}, nil,
		), nil
	}
	logging, err := newLoggingManager(config.LogConfig{}, memoryLogFactory{})
	if err != nil {
		t.Fatalf("create memory logger: %v", err)
	}
	return &productionRuntime{
		logging: logging, state: &productionState{member: daemonTestID(1)}, processGate: &localLeadershipGate{}, report: func(error) {}, firstBuild: true,
	}, &builds
}

func TestRunUsesInjectedApplicationBoundaries(t *testing.T) {
	runtime, _ := newTestProductionRuntime(t)
	loads := installRunBoundaries(t, runtime)
	if err := Run(context.Background(), Options{ConfigPath: "config.yml"}); err != nil || *loads != 2 {
		t.Fatalf("Run = %v, loads %d", err, *loads)
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
	) error {
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
