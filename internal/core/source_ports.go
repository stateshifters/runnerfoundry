package core

import (
	"context"
	"reflect"
)

// SourceEventHandler durably accepts source-neutral demand and lifecycle
// observations. Implementations must be idempotent because source adapters may
// redeliver events after acknowledgement failures.
type SourceEventHandler interface {
	IngestDemand(ctx context.Context, request JobRequest) (EnqueueResult, error)
	// ObserveRunnerAcceptance records the no-retry boundary. Redelivery must
	// return an idempotent outcome rather than create another transition.
	ObserveRunnerAcceptance(ctx context.Context, observation RunnerObservation) (ObservationResult, error)
	// ObserveTerminal ends source demand while leaving provider cleanup to runtime.
	ObserveTerminal(ctx context.Context, observation RunnerObservation) (ObservationResult, error)
}

// IsValidSourceEventHandler prevents composition from retaining an interface
// whose dynamic value is nil and would panic when an adapter delivers an event.
func IsValidSourceEventHandler(handler SourceEventHandler) bool {
	if handler == nil {
		return false
	}
	value := reflect.ValueOf(handler)
	kind := value.Kind()
	nilable := kind == reflect.Chan || kind == reflect.Func || kind == reflect.Interface ||
		kind == reflect.Map || kind == reflect.Pointer || kind == reflect.Slice
	return !nilable || !value.IsNil()
}

// RunnerBootstrapProvider owns opaque bootstrap creation and cleanup for jobs
// belonging to one source adapter. Cleanup must be idempotent: an already
// absent bootstrap is success because runtime reconciliation can replay it.
type RunnerBootstrapProvider interface {
	// GenerateBootstrap may create source-side state before returning an error.
	// Runtime therefore follows every failed provisioning path with cleanup.
	GenerateBootstrap(ctx context.Context, job Job, runnerName RunnerName) (Bootstrap, error)
	// CleanupBootstrap removes source-side bootstrap state for the exact job and
	// attempt. Repeating cleanup after the state is absent must succeed.
	CleanupBootstrap(ctx context.Context, job Job, runnerName RunnerName) error
}

// ProvisioningFailureReporter is an optional source capability. A nil error
// means the source durably accepted a terminal provisioning failure for the
// job's stable source identity. Repeating the same identity must be idempotent.
type ProvisioningFailureReporter interface {
	ReportProvisioningFailure(ctx context.Context, job Job, runnerName RunnerName) error
}

// SourceOperations must retain the concrete bootstrap provider so runtime can
// discover optional capabilities implemented by that same source instance.
type SourceOperations struct {
	RunnerBootstrapProvider RunnerBootstrapProvider
}

// IsValid prevents a resolver from returning an interface whose dynamic
// provider value is nil and cannot serve bootstrap operations.
func (operations SourceOperations) IsValid() bool {
	provider := operations.RunnerBootstrapProvider
	if provider == nil {
		return false
	}
	value := reflect.ValueOf(provider)
	kind := value.Kind()
	nilable := kind == reflect.Chan || kind == reflect.Func || kind == reflect.Interface ||
		kind == reflect.Map || kind == reflect.Pointer || kind == reflect.Slice
	return !nilable || !value.IsNil()
}

// SourceOperationsResolver selects source operations using the stable adapter
// namespace carried by a durable job. Unknown namespaces return an error;
// resolution never falls back and must preserve the concrete provider's
// optional capabilities.
type SourceOperationsResolver interface {
	ResolveSourceOperations(adapter SourceAdapter) (SourceOperations, error)
}

// CapacitySnapshot contains publishable availability for one Runner Pool.
// Available is never negative; adapters translate zero and positive values
// into their own protocol.
type CapacitySnapshot struct {
	RunnerPool RunnerPool
	Available  int
}

// CapacitySnapshotProvider supplies availability without exposing state
// backends, member liveness cutoffs, or capacity calculation mechanics.
type CapacitySnapshotProvider interface {
	CapacitySnapshot(ctx context.Context, runnerPool RunnerPool) (CapacitySnapshot, error)
}
