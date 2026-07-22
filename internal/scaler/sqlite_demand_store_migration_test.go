//go:build integration

package scaler

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteDemandStoreMigratesLegacyZeroIdentityDemand(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedLegacyZeroIdentityDemand(t, databasePath)
	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, reopened)
	ordered, err := reopened.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 1 || ordered[0].RequestID != 49 {
		t.Fatalf("expected migration to preserve only valid demand, got %#v", ordered)
	}
}

func seedLegacyZeroIdentityDemand(t *testing.T, databasePath string) {
	t.Helper()
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Now()
	if err := store.Apply(context.Background(), "linux", []JobDemand{
		{Repository: "repo", RequestID: 0, QueueTime: time.Time{}, Acquired: true},
		{Repository: "repo", RequestID: 49, QueueTime: queueTime, Acquired: true},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec("PRAGMA user_version = 1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDemandStoreRejectsFutureAndCorruptDatabases(t *testing.T) {
	futurePath := filepath.Join(t.TempDir(), "future.db")
	database, err := sql.Open("sqlite", futurePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteDemandStore(futurePath); err == nil {
		t.Fatal("expected future schema rejection")
	}
	corruptPath := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(corruptPath, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLiteDemandStore(corruptPath); err == nil {
		t.Fatal("expected corrupt database rejection")
	}
}

func TestSQLiteDemandStoreReportsOperationsAfterClose(t *testing.T) {
	store, openErr := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err == nil {
		t.Fatal("expected apply error")
	}
	if _, err := store.Ordered(ctx, "linux"); err == nil {
		t.Fatal("expected query error")
	}
	if err := store.RemoveRepository(ctx, "linux", "repo"); err == nil {
		t.Fatal("expected repository removal error")
	}
	if err := store.RemoveScaleSet(ctx, "linux"); err == nil {
		t.Fatal("expected scale set removal error")
	}
	if err := store.Delete(ctx, "linux", "repo", 1); err == nil {
		t.Fatal("expected delete error")
	}
	if err := store.migrate(ctx); err == nil {
		t.Fatal("expected migration error")
	}
}
