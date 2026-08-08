package daemon

import (
	"context"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

// githubDemandHandler keeps source event translation separate from durable
// demand reconciliation while presenting one process-local handler to each
// GitHub message session.
type githubDemandHandler struct {
	queue        *runtimeengine.QueueHandler
	demand       githubsource.AssignedDemandRepository
	wake         func(core.RunnerPool)
	stateBackend string
}

func newGitHubDemandHandler(
	queue *runtimeengine.QueueHandler,
	demand githubsource.AssignedDemandRepository,
	wake func(core.RunnerPool),
	stateBackend string,
) *githubDemandHandler {
	return &githubDemandHandler{queue: queue, demand: demand, wake: wake, stateBackend: stateBackend}
}

func (handler *githubDemandHandler) Request(
	ctx context.Context,
	request core.JobRequest,
) (core.EnqueueResult, error) {
	result, err := handler.queue.Request(ctx, request)
	if err != nil {
		return result, fmt.Errorf("handle GitHub job request: %w", err)
	}
	return result, nil
}

func (handler *githubDemandHandler) RunnerStarted(
	ctx context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	result, err := handler.queue.RunnerStarted(ctx, observation)
	if err != nil {
		return result, fmt.Errorf("handle GitHub runner-start observation: %w", err)
	}
	return result, nil
}

func (handler *githubDemandHandler) Terminalize(
	ctx context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	result, err := handler.queue.Terminalize(ctx, observation)
	if err != nil {
		return result, fmt.Errorf("handle GitHub terminal observation: %w", err)
	}
	return result, nil
}

func (handler *githubDemandHandler) ReconcileAssignedDemand(
	ctx context.Context,
	demand githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	result, err := handler.demand.ReconcileAssignedDemand(ctx, demand)
	if err != nil {
		stateErr := runtimeengine.NewDurableStateError(handler.stateBackend, "reconcile_assigned_demand", err)
		return result, fmt.Errorf("reconcile GitHub assigned demand: %w", stateErr)
	}
	if result.JobsAdded > 0 && handler.wake != nil {
		handler.wake(demand.RunnerPool)
	}
	return result, nil
}
