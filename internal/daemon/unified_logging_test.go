package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestLoggingGenerationCommitsOnlyAfterRuntimeActivation(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	wrapped := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events},
		prepared:   &preparedLogFake{events: &events},
	}
	if err := wrapped.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare logging generation: %v", err)
	}
	if want := []string{"prepare"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	if err := wrapped.Activate(context.Background()); err != nil {
		t.Fatalf("activate runtime generation: %v", err)
	}
	if err := wrapped.Close(context.Background()); err != nil {
		t.Fatalf("close logging generation: %v", err)
	}
	if want := []string{"prepare", "activate", "activate-runtime", "commit", "close-runtime", "close-prepared"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestLoggingGenerationClosesUnactivatedCandidateWhenRuntimeStartFails(t *testing.T) {
	t.Parallel()

	startErr := errors.New("start failed")
	events := make([]string, 0)
	wrapped := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, startErr: startErr},
		prepared:   &preparedLogFake{events: &events},
	}
	if err := wrapped.Prepare(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("start error = %v, want runtime failure", err)
	}
	if want := []string{"prepare", "close-prepared"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestLoggingGenerationReportsActivationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	events := make([]string, 0)
	wrapped := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, stopErr: wantErr},
		prepared:   &preparedLogFake{events: &events, activateErr: wantErr},
	}
	if err := wrapped.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Activate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("activation error = %v", err)
	}
}

func TestLoggingGenerationReportsRuntimeActivationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("runtime activation failed")
	events := make([]string, 0)
	wrapper := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, activateErr: wantErr},
		prepared:   &preparedLogFake{events: &events},
	}
	if err := wrapper.Activate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("runtime activation error = %v", err)
	}
}

func TestLoggingGenerationReportsCommitAndDeactivationFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("lifecycle failed")
	events := make([]string, 0)
	wrapper := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events},
		prepared:   &preparedLogFake{events: &events, commitErr: wantErr},
	}
	if err := wrapper.Activate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("commit error = %v, want %v", err, wantErr)
	}
	wrapper.generation = &orderedGenerationFake{events: &events, deactivateErr: wantErr}
	if err := wrapper.Deactivate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("deactivation error = %v, want %v", err, wantErr)
	}
	wrapper.generation = &orderedGenerationFake{events: &events}
	if err := wrapper.Deactivate(context.Background()); err != nil {
		t.Fatalf("successful deactivation: %v", err)
	}
}

func TestLoggingGenerationReportsUnactivatedStopFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	events := make([]string, 0)
	wrapper := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, stopErr: wantErr},
		prepared:   &preparedLogFake{events: &events, closeErr: wantErr},
	}
	if err := wrapper.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("unactivated stop error = %v", err)
	}
}

func TestLoggingGenerationReportsActivatedStopFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	events := make([]string, 0)
	wrapper := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, stopErr: wantErr},
		prepared:   &preparedLogFake{events: &events}, activated: true,
	}
	if err := wrapper.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("activated stop error = %v", err)
	}
}

func TestLoggingGenerationDelegatesFinalizers(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	finalizer := &supervisorFinalizerFake{runnerPool: "linux"}
	wrapper := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, finalizers: []runtimeFinalizer{finalizer}},
		prepared:   &preparedLogFake{events: &events},
		activated:  true,
	}
	selected := wrapper.Finalizers([]core.RunnerPool{"linux"})
	if len(selected) != 1 || selected[0] != finalizer {
		t.Fatalf("delegated finalizers = %#v", selected)
	}
}

type orderedGenerationFake struct {
	events        *[]string
	startErr      error
	stopErr       error
	activateErr   error
	deactivateErr error
	finalizers    []runtimeFinalizer
}

func (fake *orderedGenerationFake) Activate(context.Context) error {
	*fake.events = append(*fake.events, "activate-runtime")
	return fake.activateErr
}

func (fake *orderedGenerationFake) Start(context.Context) error {
	*fake.events = append(*fake.events, "prepare")
	return fake.startErr
}
func (fake *orderedGenerationFake) Stop(context.Context) error {
	*fake.events = append(*fake.events, "close-runtime")
	return fake.stopErr
}
func (fake *orderedGenerationFake) Prepare(ctx context.Context) error { return fake.Start(ctx) }
func (fake *orderedGenerationFake) Deactivate(context.Context) error  { return fake.deactivateErr }
func (fake *orderedGenerationFake) Close(ctx context.Context) error   { return fake.Stop(ctx) }
func (fake *orderedGenerationFake) Finalizers([]core.RunnerPool) []runtimeFinalizer {
	return fake.finalizers
}

type preparedLogFake struct {
	events      *[]string
	activateErr error
	commitErr   error
	closeErr    error
}

func (fake *preparedLogFake) Activate() error {
	*fake.events = append(*fake.events, "activate")
	return fake.activateErr
}
func (fake *preparedLogFake) Close() error {
	*fake.events = append(*fake.events, "close-prepared")
	return fake.closeErr
}
func (fake *preparedLogFake) Commit() error {
	*fake.events = append(*fake.events, "commit")
	return fake.commitErr
}
func (fake *preparedLogFake) Rollback() error {
	*fake.events = append(*fake.events, "rollback")
	return nil
}
