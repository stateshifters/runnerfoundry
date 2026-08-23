package runtime

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

// QueueHandler translates durable source assignments and cancellations into
// the source-neutral queue without owning runner execution.
type QueueHandler struct {
	store        core.JobQueueStore
	wakeUp       func(core.RunnerPool)
	stateBackend string
}

// DurableStateError identifies a failed state operation without exposing the
// backend driver's error text to observability code.
type DurableStateError struct {
	Backend   string
	Operation string
	cause     error
}

// NewDurableStateError preserves a state failure for control flow while its
// Error method keeps driver-supplied connection details out of logs.
func NewDurableStateError(backend string, operation string, cause error) error {
	return DurableStateError{Backend: backend, Operation: operation, cause: cause}
}

func (stateError DurableStateError) Error() string {
	return fmt.Sprintf("%s durable state %s failed", stateError.Backend, stateError.Operation)
}

func (stateError DurableStateError) Unwrap() error          { return stateError.cause }
func (stateError DurableStateError) StateBackend() string   { return stateError.Backend }
func (stateError DurableStateError) StateOperation() string { return stateError.Operation }

func NewQueueHandler(store core.JobQueueStore) *QueueHandler { return &QueueHandler{store: store} }

func NewQueueHandlerWithWake(store core.JobQueueStore, wakeUp func(core.RunnerPool)) *QueueHandler {
	return &QueueHandler{store: store, wakeUp: wakeUp}
}

func NewQueueHandlerWithWakeAndBackend(
	store core.JobQueueStore,
	wakeUp func(core.RunnerPool),
	stateBackend string,
) *QueueHandler {
	return &QueueHandler{store: store, wakeUp: wakeUp, stateBackend: stateBackend}
}

func (handler *QueueHandler) Request(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	result, err := handler.store.Queue(ctx, request)
	if err != nil {
		if handler.stateBackend != "" {
			return result, DurableStateError{Backend: handler.stateBackend, Operation: "enqueue_job", cause: err}
		}
		return result, fmt.Errorf("enqueue source request %q: %w", request.Source.ID, err)
	}
	if handler.wakeUp != nil && (result.Outcome == core.QueueQueued || result.Outcome == core.QueueAlreadyQueued) {
		handler.wakeUp(request.RunnerPool)
	}
	return result, nil
}

// RunnerStarted durably records that the source assigned the job to a runner.
// The store atomically resolves delayed start observations against queued work
// so callers can safely redeliver the same source event.
func (handler *QueueHandler) RunnerStarted(
	ctx context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	identity, unmatched, err := identityFromObservation(observation)
	if err != nil {
		return core.ObservationResult{}, err
	}
	if unmatched {
		return core.ObservationResult{Outcome: core.ObservationUnmatched}, nil
	}
	result, err := handler.store.MarkRunnerStarted(ctx, identity)
	if err != nil {
		return result, handler.lifecycleError("mark_runner_started", observation, err)
	}
	return result, nil
}

// Terminalize durably ends source demand. Provider cleanup remains
// owned by reconciliation, which retains processing until cleanup succeeds.
func (handler *QueueHandler) Terminalize(
	ctx context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	identity, unmatched, err := identityFromObservation(observation)
	if err != nil {
		return core.ObservationResult{}, err
	}
	if unmatched {
		return core.ObservationResult{Outcome: core.ObservationUnmatched}, nil
	}
	result, err := handler.store.Terminalize(ctx, identity)
	if err != nil {
		return result, handler.lifecycleError("terminalize_job", observation, err)
	}
	return result, nil
}

func identityFromObservation(observation core.RunnerObservation) (core.JobIdentity, bool, error) {
	identity := core.JobIdentity{Source: observation.Source}
	if !observation.Source.IsEmpty() && !observation.Source.IsValid() {
		return core.JobIdentity{}, false, core.ErrInvalidJobIdentity
	}
	jobID, legacy, err := observedJobID(observation.RunnerName)
	if err != nil {
		return core.JobIdentity{}, false, err
	}
	if legacy && observation.Source.IsEmpty() {
		return core.JobIdentity{}, true, nil
	}
	identity.JobID = jobID
	if err := identity.Validate(); err != nil {
		return core.JobIdentity{}, false, fmt.Errorf("validate lifecycle job identity: %w", err)
	}
	return identity, false, nil
}

func observedJobID(name core.RunnerName) (core.JobID, bool, error) {
	if name == "" {
		return core.JobID{}, false, nil
	}
	jobID, canonical, err := jobIDFromRunnerName(name)
	return jobID, !canonical, err
}

func (handler *QueueHandler) lifecycleError(
	operation string,
	observation core.RunnerObservation,
	cause error,
) error {
	if handler.stateBackend != "" {
		return DurableStateError{Backend: handler.stateBackend, Operation: operation, cause: cause}
	}
	return fmt.Errorf("%s source request %q runner %q: %w",
		operation, observation.Source.ID, observation.RunnerName, cause)
}
