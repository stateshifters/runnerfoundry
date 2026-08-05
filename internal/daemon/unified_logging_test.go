package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestLoggingGenerationActivatesOnlyAfterRuntimeStarts(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	wrapped := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events},
		prepared:   &preparedLogFake{events: &events},
	}
	if err := wrapped.Start(context.Background()); err != nil {
		t.Fatalf("start logging generation: %v", err)
	}
	if want := []string{"start", "activate"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	if err := wrapped.Stop(context.Background()); err != nil {
		t.Fatalf("stop logging generation: %v", err)
	}
	if want := []string{"start", "activate", "stop"}; !reflect.DeepEqual(events, want) {
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
	if err := wrapped.Start(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("start error = %v, want runtime failure", err)
	}
	if want := []string{"start", "close-prepared"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestLoggingGenerationReportsActivationAndUnactivatedStopFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	events := make([]string, 0)
	wrapped := &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, stopErr: wantErr},
		prepared:   &preparedLogFake{events: &events, activateErr: wantErr},
	}
	if err := wrapped.Start(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("activation error = %v", err)
	}
	events = nil
	wrapped = &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, stopErr: wantErr},
		prepared:   &preparedLogFake{events: &events, closeErr: wantErr},
	}
	if err := wrapped.Stop(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("unactivated stop error = %v", err)
	}
	wrapped = &loggingGeneration{
		generation: &orderedGenerationFake{events: &events, stopErr: wantErr},
		prepared:   &preparedLogFake{events: &events}, activated: true,
	}
	if err := wrapped.Stop(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("activated stop error = %v", err)
	}
}

type orderedGenerationFake struct {
	events   *[]string
	startErr error
	stopErr  error
}

func (fake *orderedGenerationFake) Start(context.Context) error {
	*fake.events = append(*fake.events, "start")
	return fake.startErr
}
func (fake *orderedGenerationFake) Stop(context.Context) error {
	*fake.events = append(*fake.events, "stop")
	return fake.stopErr
}

type preparedLogFake struct {
	events      *[]string
	activateErr error
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
