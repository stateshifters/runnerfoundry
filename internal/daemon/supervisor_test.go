package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
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
		LeaveActiveRunners,
	)
	assertNoTestError(t, err, "supervise generations")
	assertGenerationCalls(t, first, 1, 1, 1)
	assertGenerationCalls(t, second, 1, 1, 1)
	assertNotifierCalls(t, notifier, 2, 1)
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
		LeaveActiveRunners,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.starts != 1 || first.activations != 1 || first.stops != 1 {
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
	if _, err := startInitialGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, wantErr
	}, nil); !errors.Is(err, wantErr) {
		t.Fatalf("load error = %v", err)
	}
	if _, err := startInitialGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, nil
	}, func(context.Context, config.Config) (generation, error) { return nil, wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("build error = %v", err)
	}
	failed := &fakeGeneration{startErr: wantErr, stopErr: wantErr}
	if _, err := startInitialGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, nil
	}, func(context.Context, config.Config) (generation, error) { return failed, nil }); !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v", err)
	}
	failed = &fakeGeneration{activateErr: wantErr, stopErr: wantErr}
	if _, err := startInitialGeneration(context.Background(), func() (config.Config, error) {
		return config.Config{}, nil
	}, func(context.Context, config.Config) (generation, error) { return failed, nil }); !errors.Is(err, wantErr) {
		t.Fatalf("activation error = %v", err)
	}
}

func TestSupervisorReportsReplacementStopFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	active := &managedGeneration{generation: &fakeGeneration{stopErr: wantErr}}
	replacement := &managedGeneration{generation: &fakeGeneration{stopErr: wantErr}}
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: active}
	if err := replaceActiveGeneration(context.Background(), handoff, replacement); !errors.Is(err, wantErr) {
		t.Fatalf("replace error = %v", err)
	}
}

func TestSupervisorRetriesMultipleRetiredGenerationsAfterLaterSuccessfulHandoff(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	first := &fakeGeneration{stopErrors: []error{wantErr, wantErr, nil}}
	second := &fakeGeneration{stopErrors: []error{wantErr, nil}}
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: &managedGeneration{generation: first}}
	if err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: second}); !errors.Is(err, wantErr) {
		t.Fatalf("first handoff error = %v, want %v", err, wantErr)
	}
	if first.stops != 1 {
		t.Fatalf("failed retirement calls=%d", first.stops)
	}
	third := &fakeGeneration{}
	if err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: third}); !errors.Is(err, wantErr) {
		t.Fatalf("second handoff error = %v, want %v", err, wantErr)
	}
	if first.stops != 2 || second.stops != 1 {
		t.Fatalf("retirement retries first=%d second=%d", first.stops, second.stops)
	}
	fourth := &fakeGeneration{}
	if err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: fourth}); err != nil {
		t.Fatalf("later successful handoff: %v", err)
	}
	if first.stops != 3 || second.stops != 2 || third.stops != 1 {
		t.Fatalf("successful retries first=%d second=%d third=%d", first.stops, second.stops, third.stops)
	}
}

func TestSupervisorRetriesRetiredGenerationsDuringShutdown(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	retired := &fakeGeneration{stopErrors: []error{wantErr, nil}}
	active := &fakeGeneration{}
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: &managedGeneration{generation: retired}}
	if err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: active}); !errors.Is(err, wantErr) {
		t.Fatalf("handoff error = %v, want %v", err, wantErr)
	}
	if err := finalizeSupervisor(context.Background(), handoff.active, handoff.policy, nil, handoff.retiredGenerations); err != nil {
		t.Fatalf("shutdown retry: %v", err)
	}
	if retired.stops != 2 || active.stops != 1 {
		t.Fatalf("shutdown closes retired=%d active=%d", retired.stops, active.stops)
	}
}

func TestSupervisorShutdownContinuesAfterOneRetiredGenerationFails(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	failing := &fakeGeneration{stopErr: wantErr}
	successful := &fakeGeneration{}
	active := &fakeGeneration{}
	err := finalizeSupervisor(context.Background(), &managedGeneration{generation: active}, LeaveActiveRunners, nil,
		[]*managedGeneration{{generation: failing}, {generation: successful}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("shutdown error = %v, want %v", err, wantErr)
	}
	if failing.stops != 1 || successful.stops != 1 || active.stops != 1 {
		t.Fatalf("shutdown closes failing=%d successful=%d active=%d", failing.stops, successful.stops, active.stops)
	}
}

func TestSupervisorReportsReloadingNotificationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	notifier := &fakeSupervisorNotifier{reloadingErr: wantErr}
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: &managedGeneration{generation: &fakeGeneration{}}}
	if err := reloadGeneration(context.Background(), nil, nil, notifier, handoff); !errors.Is(err, wantErr) {
		t.Fatalf("reloading notification error = %v", err)
	}
}

func TestSupervisorReportsReplacementReadyAndHandoffFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: &managedGeneration{generation: &fakeGeneration{}}}
	notifier := &fakeSupervisorNotifier{readyErr: wantErr}
	loads := 0
	load := func() (config.Config, error) { loads++; return config.Config{}, nil }
	build := func(context.Context, config.Config) (generation, error) { return &fakeGeneration{}, nil }
	if err := reloadGeneration(context.Background(), load, build, notifier, handoff); !errors.Is(err, wantErr) {
		t.Fatalf("ready notification error = %v", err)
	}
	if loads != 1 {
		t.Fatalf("loads = %d", loads)
	}
	handoff.active = &managedGeneration{generation: &fakeGeneration{stopErr: wantErr}}
	if err := reloadGeneration(context.Background(), load, build, &fakeSupervisorNotifier{}, handoff); !errors.Is(err, wantErr) {
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
		&fakeSupervisorNotifier{readyErr: wantErr}, func(error) {}, LeaveActiveRunners)
	if !errors.Is(err, wantErr) || active.stops != 1 {
		t.Fatalf("supervise error=%v generation=%+v", err, active)
	}
}

func TestSupervisorReportsInitialStartFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("load failed")
	err := supervise(context.Background(), nil,
		func() (config.Config, error) { return config.Config{}, wantErr }, nil,
		&fakeSupervisorNotifier{}, func(error) {}, LeaveActiveRunners)
	if !errors.Is(err, wantErr) {
		t.Fatalf("supervise error = %v", err)
	}
}

type fakeGeneration struct {
	starts        int
	activations   int
	deactivations int
	stops         int
	onStart       func()
	onActivate    func()
	startErr      error
	activateErr   error
	stopErr       error
	stopErrors    []error
	finalizers    []runtimeFinalizer
}

func (generation *fakeGeneration) Prepare(ctx context.Context) error { return generation.Start(ctx) }
func (generation *fakeGeneration) Deactivate(context.Context) error {
	generation.deactivations++
	return nil
}
func (generation *fakeGeneration) Close(ctx context.Context) error { return generation.Stop(ctx) }

func assertGenerationCalls(t *testing.T, candidate *fakeGeneration, starts int, activations int, stops int) {
	t.Helper()
	if candidate.starts != starts || candidate.activations != activations || candidate.stops != stops {
		t.Fatalf("generation calls = %+v, want starts=%d activations=%d stops=%d", candidate, starts, activations, stops)
	}
}

func assertNotifierCalls(t *testing.T, notifier *fakeSupervisorNotifier, ready int, reloading int) {
	t.Helper()
	if notifier.ready != ready || notifier.reloading != reloading {
		t.Fatalf("notification calls = %+v, want ready=%d reloading=%d", notifier, ready, reloading)
	}
}

func (generation *fakeGeneration) Activate(context.Context) error {
	generation.activations++
	if generation.onActivate != nil {
		generation.onActivate()
	}
	return generation.activateErr
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
	if generation.stops <= len(generation.stopErrors) {
		return generation.stopErrors[generation.stops-1]
	}
	return generation.stopErr
}

func (generation *fakeGeneration) Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer {
	wanted := make(map[core.RunnerPool]struct{}, len(runnerPools))
	for _, runnerPool := range runnerPools {
		wanted[runnerPool] = struct{}{}
	}
	selected := make([]runtimeFinalizer, 0, len(generation.finalizers))
	for _, finalizer := range generation.finalizers {
		if _, exists := wanted[finalizer.RunnerPool()]; exists {
			selected = append(selected, finalizer)
		}
	}
	return selected
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

func (*fakeSupervisorNotifier) Status(context.Context, operationalStatus) error { return nil }
