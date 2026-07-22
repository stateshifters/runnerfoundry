//go:build integration

package scaler

import (
	"context"
	"testing"
)

func seedRecoveryDemandStore(t *testing.T, databasePath string, demandByScaleSet map[string][]JobDemand) {
	t.Helper()
	store := openRecoveryStore(t, databasePath)
	for scaleSet, demand := range demandByScaleSet {
		if err := store.Apply(context.Background(), scaleSet, demand, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func reopenedRecoveryStore(t *testing.T, databasePath string) *SQLiteDemandStore {
	t.Helper()
	store := openRecoveryStore(t, databasePath)
	closeSQLiteDemandStore(t, store)
	return store
}

func assertSingleRecoveryDemand(t *testing.T, store *SQLiteDemandStore, scaleSet string, requestID int64) QueuedDemand {
	t.Helper()
	demand, err := store.Ordered(context.Background(), scaleSet)
	if err != nil || len(demand) != 1 || demand[0].RequestID != requestID {
		t.Fatalf("%s recovery demand = %#v, %v; want request %d", scaleSet, demand, err, requestID)
	}
	return demand[0]
}
