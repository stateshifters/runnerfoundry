//go:build integration

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

type startupResult struct {
	lease *LeaderLease
	err   error
}

func TestPostgreSQLConcurrentStartupMigratesOnceAndElectsOneLeader(t *testing.T) {
	firstStore, _ := postgreSQLTestStore(t)
	if err := firstStore.MigrateDown(context.Background(), 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	stores := []*Store{firstStore, NewStore(firstStore.settings)}
	leaders := collectStartupLeaders(t, startStoresConcurrently(stores), len(stores))
	if len(leaders) != 1 {
		t.Fatalf("leaders after simultaneous startup = %d, backend IDs = %v", len(leaders), leaderBackendIDs(t, leaders))
	}
	if err := leaders[0].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := firstStore.MigrationStatus(context.Background())
	if err != nil || status.Current != 1 {
		t.Fatalf("migration status after simultaneous startup = %#v, %v", status, err)
	}
}

func startStoresConcurrently(stores []*Store) <-chan startupResult {
	results := make(chan startupResult, len(stores))
	for _, store := range stores {
		go func(candidate *Store) {
			if err := candidate.Migrate(context.Background()); err != nil {
				results <- startupResult{err: err}
				return
			}
			lease, err := candidate.TryLeader(context.Background())
			results <- startupResult{lease: lease, err: err}
		}(store)
	}
	return results
}

func collectStartupLeaders(t *testing.T, results <-chan startupResult, count int) []*LeaderLease {
	t.Helper()
	leaders := make([]*LeaderLease, 0, 1)
	for range count {
		result := <-results
		if result.err != nil && !errors.Is(result.err, ErrNotLeader) {
			t.Fatal(result.err)
		}
		if result.lease != nil {
			leaders = append(leaders, result.lease)
		}
	}
	return leaders
}

func leaderBackendIDs(t *testing.T, leaders []*LeaderLease) []int {
	t.Helper()
	backendIDs := make([]int, 0, len(leaders))
	for _, leader := range leaders {
		var backendID int
		if err := leader.connection.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&backendID); err != nil {
			t.Fatal(err)
		}
		backendIDs = append(backendIDs, backendID)
	}
	return backendIDs
}
