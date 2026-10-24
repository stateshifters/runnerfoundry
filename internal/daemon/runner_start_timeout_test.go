package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestConfiguredPoolsUseIndependentStartTimeouts(t *testing.T) {
	resolver, err := newSourceOperationsResolver([]sourceOperationsRegistration{{
		adapter: "test", operations: &sourceBootstrapProviderFake{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	assembled := assembledRuntime{sourceOperations: resolver}
	for _, test := range []struct {
		name          string
		timeout       string
		age           time.Duration
		wantRecovered int
	}{
		{name: "fast", timeout: "1m", age: 2 * time.Minute, wantRecovered: 1},
		{name: "slow", timeout: "10m", age: 2 * time.Minute},
		{name: "default", age: 6 * time.Minute, wantRecovered: 1},
	} {
		configured := builderConfig().RunnerPools[0]
		configured.Name = test.name
		configured.StopIfNotStartedWithin = test.timeout
		recovered := reconcileConfiguredStartTimeout(t, &assembled, configured, test.age)
		if recovered != test.wantRecovered {
			t.Fatalf("pool %q recovered %d jobs, want %d", test.name, recovered, test.wantRecovered)
		}
	}
}

func reconcileConfiguredStartTimeout(t *testing.T, assembled *assembledRuntime, configured config.RunnerPoolConfig, age time.Duration) int {
	t.Helper()
	runnerPool := core.RunnerPool(configured.Name)
	store := &startTimeoutStore{builderStoreFake: newBuilderStoreFake(), processing: core.ProcessingJob{
		Job:       core.Job{ID: uuid.New(), RunnerPool: runnerPool, Source: core.SourceReference{Adapter: "test", ID: "request"}},
		ClaimedAt: builderTime().Add(-age), InventoryEndpointID: "test:builder",
	}}
	dependencies := unifiedBuilderDependencies{
		state:     runtimeStatePorts{processing: store, transitions: store, provisioningFailures: store},
		providers: map[core.RunnerPool]execution.RunnerLauncher{runnerPool: builderProviderFake{}},
		inventory: builderProviderFake{}, member: daemonTestID(1), currentTime: builderTime,
	}
	if err := assembled.appendRunnerPool(configured, dependencies); err != nil {
		t.Fatal(err)
	}
	pool := assembled.pools[len(assembled.pools)-1].runtime
	if err := pool.Reconcile(context.Background(), runnerPool); err != nil {
		t.Fatal(err)
	}
	return store.recovered
}

func TestBuildRejectsInvalidRunnerStartTimeout(t *testing.T) {
	configured := builderConfig()
	configured.RunnerPools[0].StopIfNotStartedWithin = "invalid"
	assertBuildRejected(t, configured, validBuilderDependencies(t))
}

type startTimeoutStore struct {
	*builderStoreFake
	processing core.ProcessingJob
	recovered  int
}

func (store *startTimeoutStore) ProcessingByMember(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
	return []core.ProcessingJob{store.processing}, nil
}

func (store *startTimeoutStore) ProcessingByPool(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
	return []core.ProcessingJob{store.processing}, nil
}

func (store *startTimeoutStore) RecoverUnstartedProcessing(context.Context, core.ProcessingJob) (core.RetryResult, error) {
	store.recovered++
	return core.RetryResult{Outcome: core.RetryRequeued}, nil
}
