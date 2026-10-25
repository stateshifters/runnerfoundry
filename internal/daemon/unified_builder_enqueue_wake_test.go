package daemon

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestBuiltSQLiteGenerationRequestsQueuedWorkWithoutExecutionTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &enqueueWakeStore{
			builderStoreFake: newBuilderStoreFake(), claims: make(chan core.RunnerPool, 2),
		}
		manager := &enqueueWakeManager{
			Manager: newBuilderManager(t, store.builderStoreFake), handler: make(chan core.SourceEventHandler, 1),
		}
		startEnqueueWakeGeneration(t, store, manager)
		synctest.Wait()
		assertEnqueueWakeClaim(t, store.claims)
		var handler core.SourceEventHandler
		select {
		case handler = <-manager.handler:
		default:
			t.Fatal("Git source session did not receive an event handler")
		}
		result, err := handler.IngestDemand(context.Background(), core.JobRequest{
			Source:     core.SourceReference{Adapter: githubsource.AdapterName, ID: "queued-request"},
			RunnerPool: "linux", EnqueuedAt: builderTime(),
		})
		if err != nil || result.Outcome != core.QueueQueued {
			t.Fatalf("ingest demand = %+v, %v, want queued", result, err)
		}
		synctest.Wait()
		assertEnqueueWakeClaim(t, store.claims)
	})
}

func startEnqueueWakeGeneration(t *testing.T, store *enqueueWakeStore, manager *enqueueWakeManager) {
	t.Helper()
	dependencies := validBuilderDependencies(t)
	dependencies.state = builderStatePorts(store.builderStoreFake)
	dependencies.state.jobs = store
	dependencies.state.processing = store
	dependencies.manager = manager
	generation, err := buildUnifiedGeneration(builderConfig(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := generation.Close(context.Background()); closeErr != nil {
			t.Fatal(closeErr)
		}
	})
	if err = generation.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = generation.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func assertEnqueueWakeClaim(t *testing.T, claims <-chan core.RunnerPool) {
	t.Helper()
	select {
	case runnerPool := <-claims:
		if runnerPool != "linux" {
			t.Fatalf("requested work for Runner Pool %q, want linux", runnerPool)
		}
	default:
		t.Fatal("generation did not request queued work")
	}
}

type enqueueWakeStore struct {
	*builderStoreFake
	claims chan core.RunnerPool
}

func (*enqueueWakeStore) Queue(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{Outcome: core.QueueQueued}, nil
}

func (store *enqueueWakeStore) ClaimOldest(
	_ context.Context,
	runnerPool core.RunnerPool,
	_ core.ProcessingClaim,
) (core.ClaimResult, error) {
	store.claims <- runnerPool
	return core.ClaimResult{Outcome: core.ClaimEmpty}, nil
}

type enqueueWakeManager struct {
	*githubsource.Manager
	handler chan core.SourceEventHandler
}

func (*enqueueWakeManager) ActiveRegistrations(context.Context) ([]githubsource.Registration, error) {
	return []githubsource.Registration{{RepositoryURL: "https://github.com/test/project", RunnerPool: "linux"}}, nil
}

func (manager *enqueueWakeManager) OpenSourceRegistrationSession(
	ctx context.Context,
	_ githubsource.Registration,
	_ githubsource.RunnerPoolConfig,
	_ string,
	handler core.SourceEventHandler,
	_ githubsource.AssignedDemandHandler,
) (*githubsource.RegistrationSession, error) {
	manager.handler <- handler
	<-ctx.Done()
	return nil, fmt.Errorf("open scripted source session: %w", ctx.Err())
}
