package core

import (
	"context"
	"testing"
)

func TestSourceOperationsRejectNilProvidersAndAcceptCallableValues(t *testing.T) {
	var typedNil *valueBootstrapProvider
	for name, operations := range map[string]SourceOperations{
		"nil interface":  {},
		"typed nil":      {RunnerBootstrapProvider: typedNil},
		"callable value": {RunnerBootstrapProvider: valueBootstrapProvider{}},
	} {
		t.Run(name, func(t *testing.T) {
			wantValid := name == "callable value"
			if operations.IsValid() != wantValid {
				t.Fatalf("IsValid() = %t, want %t", operations.IsValid(), wantValid)
			}
		})
	}
}

func TestSourceEventHandlerValidityRejectsTypedNil(t *testing.T) {
	var typedNil *valueSourceEventHandler
	for name, handler := range map[string]SourceEventHandler{
		"nil interface":  nil,
		"typed nil":      typedNil,
		"callable value": valueSourceEventHandler{},
	} {
		t.Run(name, func(t *testing.T) {
			wantValid := name == "callable value"
			if IsValidSourceEventHandler(handler) != wantValid {
				t.Fatalf("IsValidSourceEventHandler() = %t, want %t",
					IsValidSourceEventHandler(handler), wantValid)
			}
		})
	}
}

type valueSourceEventHandler struct{}

func (valueSourceEventHandler) IngestDemand(context.Context, JobRequest) (EnqueueResult, error) {
	return EnqueueResult{}, nil
}

func (valueSourceEventHandler) ObserveRunnerAcceptance(
	context.Context,
	RunnerObservation,
) (ObservationResult, error) {
	return ObservationResult{}, nil
}

func (valueSourceEventHandler) ObserveTerminal(
	context.Context,
	RunnerObservation,
) (ObservationResult, error) {
	return ObservationResult{}, nil
}

type valueBootstrapProvider struct{}

func (valueBootstrapProvider) GenerateBootstrap(context.Context, Job, RunnerName) (Bootstrap, error) {
	return Bootstrap{}, nil
}

func (valueBootstrapProvider) CleanupBootstrap(context.Context, Job, RunnerName) error { return nil }
