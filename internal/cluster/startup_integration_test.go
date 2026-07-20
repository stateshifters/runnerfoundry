//go:build integration

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgreSQLConcurrentStartupMigratesOnceAndElectsOneLeader(t *testing.T) {
	firstStore, _ := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := firstStore.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	stores := []*Store{firstStore, NewStore(firstStore.settings)}
	type startupResult struct {
		lease *LeaderLease
		err   error
	}
	results := make(chan startupResult, len(stores))
	for _, store := range stores {
		go func(candidate *Store) {
			if err := candidate.Migrate(ctx); err != nil {
				results <- startupResult{err: err}
				return
			}
			lease, err := candidate.TryLeader(ctx)
			results <- startupResult{lease: lease, err: err}
		}(store)
	}
	leaders := make([]*LeaderLease, 0, 1)
	for range stores {
		result := <-results
		if result.err != nil && !errors.Is(result.err, ErrNotLeader) {
			t.Fatal(result.err)
		}
		if result.lease != nil {
			leaders = append(leaders, result.lease)
		}
	}
	if len(leaders) != 1 {
		backendIDs := make([]int, 0, len(leaders))
		for _, leader := range leaders {
			var backendID int
			if queryErr := leader.connection.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&backendID); queryErr != nil {
				t.Fatal(queryErr)
			}
			backendIDs = append(backendIDs, backendID)
		}
		t.Fatalf("leaders after simultaneous startup = %d, backend IDs = %v", len(leaders), backendIDs)
	}
	if err := leaders[0].Close(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := firstStore.MigrationStatus(ctx)
	if err != nil || status.Current != 1 {
		t.Fatalf("migration status after simultaneous startup = %#v, %v", status, err)
	}
}
