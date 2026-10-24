//go:build e2e

package daemon

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/config"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

type stateConformanceStore struct {
	statecontract.JobQueueStore
	statecontract.ProcessingJobStore
	statecontract.ProcessingTransitions
	statecontract.ProvisioningFailureTransitions
	statecontract.MemberRegistry
	statecontract.RunnerPoolCapacityStore
	statecontract.DemandTransactionStore
	statecontract.MemberLivenessReader
}

func sqliteStateConformanceStore(store *sqlitestate.Store) stateConformanceStore {
	return stateConformanceStore{
		JobQueueStore:                  store,
		ProcessingJobStore:             store,
		ProcessingTransitions:          store,
		ProvisioningFailureTransitions: store,
		MemberRegistry:                 store,
		RunnerPoolCapacityStore:        store,
		DemandTransactionStore:         store,
		MemberLivenessReader:           store,
	}
}

func postgresStateConformanceStore(store *postgresstate.Store) stateConformanceStore {
	return stateConformanceStore{
		JobQueueStore:                  store,
		ProcessingJobStore:             store,
		ProcessingTransitions:          store,
		ProvisioningFailureTransitions: store,
		MemberRegistry:                 store,
		RunnerPoolCapacityStore:        store,
		DemandTransactionStore:         store,
		MemberLivenessReader:           store,
	}
}

func TestStateStoreContractSQLite(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	store, err := sqlitestate.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open SQLite state conformance store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close SQLite state conformance store: %v", closeErr)
		}
	})
	exerciseStateStoreContract(t, sqliteStateConformanceStore(store))
}

func TestStateStoreContractPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	store := openPostgresStateConformanceStore(t)
	exerciseStateStoreContract(t, postgresStateConformanceStore(store))
}

func openPostgresStateConformanceStore(t *testing.T) *postgresstate.Store {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	dependencies := newProductionDependencies()
	dependencies.readHostname = func() (string, error) { return "state-conformance-member", nil }
	opened, err := openProductionStateWithDependencies(context.Background(), config.Config{
		State: config.StateConfig{Postgres: &settings},
	}, dependencies)
	if err != nil {
		t.Fatalf("open PostgreSQL state conformance store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := closeRuntimeResources(context.Background(), opened.closers); closeErr != nil {
			t.Errorf("close PostgreSQL state conformance store: %v", closeErr)
		}
	})
	store, ok := opened.store.members.(*postgresstate.Store)
	if !ok {
		t.Fatalf("PostgreSQL production member state store type = %T", opened.store.members)
	}
	return store
}

func exerciseStateStoreContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	t.Run("queue ordering replay retry and attempt ownership", func(t *testing.T) {
		exerciseQueueRetryContract(t, store)
	})
	t.Run("delayed and repeated lifecycle observations", func(t *testing.T) {
		exerciseLifecycleObservationContract(t, store)
	})
	t.Run("conditional terminal observations", func(t *testing.T) {
		exerciseConditionalTerminalContract(t, store)
	})
	t.Run("provisioning failure reporting phases", func(t *testing.T) {
		exerciseProvisioningFailureContract(t, store)
	})
	t.Run("member liveness withdrawal and capacity", func(t *testing.T) {
		exerciseMembershipCapacityContract(t, store)
	})
	t.Run("typed active demand removal and rollback", func(t *testing.T) {
		exerciseDemandTransactionContract(t, store)
	})
}
