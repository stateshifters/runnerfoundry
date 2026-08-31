package runtime

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

// QueueHandler translates durable source assignments and cancellations into
// the source-neutral queue without owning runner execution.
type QueueHandler struct {
	store        state.JobQueueStore
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

func NewQueueHandler(store state.JobQueueStore) *QueueHandler { return &QueueHandler{store: store} }

// NewQueueHandlerWithWake signals the affected Runner Pool after demand is
// newly queued or replayed while already queued.
func NewQueueHandlerWithWake(store state.JobQueueStore, wakeUp func(core.RunnerPool)) *QueueHandler {
	return &QueueHandler{store: store, wakeUp: wakeUp}
}

// NewQueueHandlerWithWakeAndBackend adds a stable backend name to durable-state
// failures while keeping the driver error available through errors.Unwrap.
func NewQueueHandlerWithWakeAndBackend(
	store state.JobQueueStore,
	wakeUp func(core.RunnerPool),
	stateBackend string,
) *QueueHandler {
	return &QueueHandler{store: store, wakeUp: wakeUp, stateBackend: stateBackend}
}

// IngestDemand queues source demand idempotently and wakes its Runner Pool only
// when the request remains eligible for processing.
func (handler *QueueHandler) IngestDemand(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
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

// ObserveRunnerAcceptance atomically resolves delayed acceptance observations
// so callers can safely redeliver the same source event.
func (handler *QueueHandler) ObserveRunnerAcceptance(
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
	result, err := handler.store.ObserveRunnerAcceptance(ctx, identity)
	if err != nil {
		return result, handler.lifecycleError("mark_runner_accepted", observation, err)
	}
	return result, nil
}

// ObserveTerminal durably ends source demand. Provider cleanup remains
// owned by reconciliation, which retains processing until cleanup succeeds.
func (handler *QueueHandler) ObserveTerminal(
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
	runnerIdentity, unowned, err := observedRunnerIdentity(observation.RunnerName)
	if err != nil {
		return core.JobIdentity{}, false, err
	}
	if unowned && observation.Source.IsEmpty() {
		return core.JobIdentity{}, true, nil
	}
	identity.JobID = runnerIdentity.JobID
	identity.AttemptID = runnerIdentity.AttemptID
	if err := identity.Validate(); err != nil {
		return core.JobIdentity{}, false, fmt.Errorf("validate lifecycle job identity: %w", err)
	}
	return identity, false, nil
}

func observedRunnerIdentity(name core.RunnerName) (core.RunnerIdentity, bool, error) {
	if name == "" {
		return core.RunnerIdentity{}, false, nil
	}
	identity, owned, err := core.ParseRunnerName(name)
	if err != nil {
		return core.RunnerIdentity{}, false, fmt.Errorf("parse observed runner identity: %w", err)
	}
	return identity, !owned, nil
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
