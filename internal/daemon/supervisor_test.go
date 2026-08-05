package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestSupervisorReplacesGenerationOnlyAfterReplacementStarts(t *testing.T) {
	first := &fakeGeneration{}
	second := &fakeGeneration{}
	builds := []*fakeGeneration{first, second}
	buildIndex := 0
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	canceled, cancel := context.WithCancel(context.Background())
	second.onStart = cancel

	notifier := &fakeSupervisorNotifier{}
	err := supervise(canceled, reloads,
		func() (config.Config, error) { return config.Config{}, nil },
		func(context.Context, config.Config) (generation, error) {
			built := builds[buildIndex]
			buildIndex++
			return built, nil
		},
		notifier,
		func(error) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.starts != 1 || first.stops != 1 || first.shutdowns != 0 ||
		second.starts != 1 || second.stops != 0 || second.shutdowns != 1 {
		t.Fatalf("generation calls first=%+v second=%+v", first, second)
	}
	if notifier.ready != 2 || notifier.reloading != 1 {
		t.Fatalf("notification calls = %+v", notifier)
	}
}

func TestSupervisorKeepsActiveGenerationWhenReloadBuildFails(t *testing.T) {
	first := &fakeGeneration{}
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	canceled, cancel := context.WithCancel(context.Background())
	wantErr := errors.New("replacement invalid")
	builder := failingReplacementBuilder(first, wantErr, cancel)

	var reported error
	notifier := &fakeSupervisorNotifier{}
	err := supervise(canceled, reloads,
		func() (config.Config, error) { return config.Config{}, nil },
		builder,
		notifier,
		func(err error) { reported = err },
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.starts != 1 || first.stops != 0 || first.shutdowns != 1 {
		t.Fatalf("active generation calls = %+v", first)
	}
	if !errors.Is(reported, wantErr) {
		t.Fatalf("reported error = %v, want %v", reported, wantErr)
	}
	if notifier.ready != 2 || notifier.reloading != 1 {
		t.Fatalf("notification calls = %+v, want READY restored after failed reload", notifier)
	}
}

func failingReplacementBuilder(active generation, replacementErr error, onReplacement func()) generationBuilder {
	builds := 0
	return func(context.Context, config.Config) (generation, error) {
		builds++
		if builds == 1 {
			return active, nil
		}
		onReplacement()
		return nil, replacementErr
	}
}

func TestGenerationConfigLoaderRejectsStateBackendChanges(t *testing.T) {
	t.Parallel()

	initial := config.Config{State: config.StateConfig{Path: "runnerfoundry.db"}}
	replacement := initial
	replacement.State.Path = "replacement.db"
	loader := newGenerationConfigLoader(initial, func() (config.Config, error) { return replacement, nil })
	if loaded, err := loader(); err != nil || loaded.State.Path != initial.State.Path {
		t.Fatalf("initial load = %#v, %v", loaded, err)
	}
	if _, err := loader(); err == nil {
		t.Fatal("reload accepted a state backend change")
	}
}

func TestSupervisorReportsStartupFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	if _, err := startGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, wantErr
	}, nil, "initial"); !errors.Is(err, wantErr) {
		t.Fatalf("load error = %v", err)
	}
	if _, err := startGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, nil
	}, func(context.Context, config.Config) (generation, error) { return nil, wantErr }, "initial"); !errors.Is(err, wantErr) {
		t.Fatalf("build error = %v", err)
	}
	failed := &fakeGeneration{startErr: wantErr, stopErr: wantErr}
	if _, err := startGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, nil
	}, func(context.Context, config.Config) (generation, error) { return failed, nil }, "initial"); !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v", err)
	}
}

func TestSupervisorReportsNotificationAndReplacementFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	active := &managedGeneration{generation: &fakeGeneration{stopErr: wantErr}}
	replacement := &managedGeneration{generation: &fakeGeneration{stopErr: wantErr}}
	if err := replaceActiveGeneration(context.Background(), &active, replacement); !errors.Is(err, wantErr) {
		t.Fatalf("replace error = %v", err)
	}
	notifier := &fakeSupervisorNotifier{reloadingErr: wantErr}
	if err := reloadGeneration(context.Background(), nil, nil, notifier, &active); !errors.Is(err, wantErr) {
		t.Fatalf("reloading notification error = %v", err)
	}
	active = &managedGeneration{generation: &fakeGeneration{}}
	notifier = &fakeSupervisorNotifier{readyErr: wantErr}
	loads := 0
	load := func() (config.Config, error) { loads++; return config.Config{}, nil }
	build := func(context.Context, config.Config) (generation, error) { return &fakeGeneration{}, nil }
	if err := reloadGeneration(context.Background(), load, build, notifier, &active); !errors.Is(err, wantErr) {
		t.Fatalf("ready notification error = %v", err)
	}
	if loads != 1 {
		t.Fatalf("loads = %d", loads)
	}
	active = &managedGeneration{generation: &fakeGeneration{stopErr: wantErr}}
	if err := reloadGeneration(context.Background(), load, build, &fakeSupervisorNotifier{}, &active); !errors.Is(err, wantErr) {
		t.Fatalf("replacement handoff error = %v", err)
	}
}

func TestSupervisorReportsInitialReadyFailureAndStopsGeneration(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("ready failed")
	active := &fakeGeneration{}
	err := supervise(context.Background(), nil,
		func() (config.Config, error) { return config.Config{}, nil },
		func(context.Context, config.Config) (generation, error) { return active, nil },
		&fakeSupervisorNotifier{readyErr: wantErr}, func(error) {})
	if !errors.Is(err, wantErr) || active.shutdowns != 1 {
		t.Fatalf("supervise error=%v generation=%+v", err, active)
	}
}

func TestSupervisorReportsInitialStartFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("load failed")
	err := supervise(context.Background(), nil,
		func() (config.Config, error) { return config.Config{}, wantErr }, nil,
		&fakeSupervisorNotifier{}, func(error) {})
	if !errors.Is(err, wantErr) {
		t.Fatalf("supervise error = %v", err)
	}
}

type fakeGeneration struct {
	starts    int
	stops     int
	shutdowns int
	onStart   func()
	startErr  error
	stopErr   error
}

func (generation *fakeGeneration) Start(context.Context) error {
	generation.starts++
	if generation.onStart != nil {
		generation.onStart()
	}
	return generation.startErr
}

func (generation *fakeGeneration) Stop(context.Context) error {
	generation.stops++
	return generation.stopErr
}

func (generation *fakeGeneration) Shutdown(context.Context) error {
	generation.shutdowns++
	return generation.stopErr
}

type fakeSupervisorNotifier struct {
	ready        int
	reloading    int
	readyErr     error
	reloadingErr error
}

func (notifier *fakeSupervisorNotifier) Ready(context.Context) error {
	notifier.ready++
	return notifier.readyErr
}

func (notifier *fakeSupervisorNotifier) Reloading(context.Context) error {
	notifier.reloading++
	return notifier.reloadingErr
}
