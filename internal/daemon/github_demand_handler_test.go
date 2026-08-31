package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestGitHubDemandHandlerWakesOnlyAfterCommittedAddition(t *testing.T) {
	runnerPool := core.RunnerPool("linux")
	repository := &assignedDemandRepositoryFake{
		result: githubsource.AssignedDemandResult{ActiveJobs: 3, JobsAdded: 2},
	}
	wakes := make([]core.RunnerPool, 0, 1)
	handler := newGitHubDemandHandler(
		runtimeengine.NewQueueHandler(nil), repository,
		func(pool core.RunnerPool) { wakes = append(wakes, pool) }, "postgresql",
	)

	result, err := handler.ReconcileAssignedDemand(context.Background(), githubsource.AssignedDemand{
		Repository: "https://github.com/example/project", RunnerPool: runnerPool,
		ObservationID: "message:17", DesiredJobs: 3,
	})
	if err != nil || result != repository.result {
		t.Fatalf("reconcile assigned demand = (%+v, %v), want (%+v, nil)", result, err, repository.result)
	}
	if len(wakes) != 1 || wakes[0] != runnerPool {
		t.Fatalf("wakes = %v, want [%s]", wakes, runnerPool)
	}
}

func TestGitHubDemandHandlerDoesNotWakeAfterFailureOrRemoval(t *testing.T) {
	runnerPool := core.RunnerPool("linux")
	wakes := 0
	repository := &assignedDemandRepositoryFake{err: errors.New("state unavailable")}
	handler := newGitHubDemandHandler(
		runtimeengine.NewQueueHandler(nil), repository,
		func(core.RunnerPool) { wakes++ }, "sqlite",
	)
	demand := githubsource.AssignedDemand{Repository: "https://github.com/example/project", RunnerPool: runnerPool,
		ObservationID: "message:17", DesiredJobs: 1}

	_, err := handler.ReconcileAssignedDemand(context.Background(), demand)
	var stateErr runtimeengine.DurableStateError
	if !errors.As(err, &stateErr) || stateErr.Backend != "sqlite" || stateErr.Operation != "reconcile_assigned_demand" {
		t.Fatalf("error = %v, want SQLite reconcile_assigned_demand durable-state error", err)
	}
	if !errors.Is(err, repository.err) {
		t.Fatalf("error = %v, want wrapped repository failure", err)
	}
	repository.err = nil
	repository.result = githubsource.AssignedDemandResult{ActiveJobs: 1, JobsRemoved: 1}
	if _, err := handler.ReconcileAssignedDemand(context.Background(), demand); err != nil {
		t.Fatalf("remove assigned demand: %v", err)
	}
	if wakes != 0 {
		t.Fatalf("wakes = %d, want 0", wakes)
	}
}

func TestGitHubDemandHandlerDelegatesExactLifecycleEvents(t *testing.T) {
	store := &githubDemandQueueStoreFake{}
	handler := newGitHubDemandHandler(runtimeengine.NewQueueHandler(store), &assignedDemandRepositoryFake{}, nil, "")
	source := core.SourceReference{Adapter: "github", ID: "request"}
	request := core.JobRequest{Source: source, RunnerPool: "linux"}
	observation := core.RunnerObservation{Source: source}
	assertGitHubDemandLifecycleSuccess(t, handler, request, observation)

	store.err = errors.New("state failed")
	assertGitHubDemandLifecycleFailure(t, handler, request, observation, store.err)
}

func assertGitHubDemandLifecycleSuccess(
	t *testing.T,
	handler *githubDemandHandler,
	request core.JobRequest,
	observation core.RunnerObservation,
) {
	t.Helper()
	if result, err := handler.IngestDemand(context.Background(), request); err != nil || result.Outcome != core.QueueQueued {
		t.Fatalf("request = (%#v, %v)", result, err)
	}
	if result, err := handler.ObserveRunnerAcceptance(context.Background(), observation); err != nil ||
		result.Outcome != core.ObservationApplied {
		t.Fatalf("runner acceptance = (%#v, %v)", result, err)
	}
	if result, err := handler.ObserveTerminal(context.Background(), observation); err != nil ||
		result.Outcome != core.ObservationApplied {
		t.Fatalf("terminalize = (%#v, %v)", result, err)
	}
}

func assertGitHubDemandLifecycleFailure(
	t *testing.T,
	handler *githubDemandHandler,
	request core.JobRequest,
	observation core.RunnerObservation,
	wantErr error,
) {
	t.Helper()
	if _, err := handler.IngestDemand(context.Background(), request); !errors.Is(err, wantErr) {
		t.Fatalf("request error = %v", err)
	}
	if _, err := handler.ObserveRunnerAcceptance(context.Background(), observation); !errors.Is(err, wantErr) {
		t.Fatalf("runner-acceptance error = %v", err)
	}
	if _, err := handler.ObserveTerminal(context.Background(), observation); !errors.Is(err, wantErr) {
		t.Fatalf("terminal error = %v", err)
	}
}

type assignedDemandRepositoryFake struct {
	result githubsource.AssignedDemandResult
	err    error
}

type githubDemandQueueStoreFake struct{ err error }

func (store *githubDemandQueueStoreFake) Queue(
	context.Context,
	core.JobRequest,
) (core.EnqueueResult, error) {
	return core.EnqueueResult{Outcome: core.QueueQueued}, store.err
}

func (store *githubDemandQueueStoreFake) ObserveRunnerAcceptance(
	context.Context,
	core.JobIdentity,
) (core.ObservationResult, error) {
	return core.ObservationResult{Outcome: core.ObservationApplied}, store.err
}

func (store *githubDemandQueueStoreFake) Terminalize(
	context.Context,
	core.JobIdentity,
) (core.ObservationResult, error) {
	return core.ObservationResult{Outcome: core.ObservationApplied}, store.err
}

func (repository *assignedDemandRepositoryFake) ReconcileAssignedDemand(
	context.Context,
	githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	return repository.result, repository.err
}
