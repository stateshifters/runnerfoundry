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

func TestSQLiteDemandStoreMigratesLibvirtLifecycleToGenericLifecycle(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.database.Exec(`DROP TRIGGER mirror_libvirt_runner_lifecycle_insert;
		DROP TRIGGER mirror_libvirt_runner_lifecycle_update;
		DROP TRIGGER mirror_libvirt_runner_lifecycle_delete;
		DELETE FROM runner_lifecycle;
		INSERT INTO libvirt_runner_lifecycle(scale_set, repository, request_id, runner_name, reap_requested)
		VALUES('linux', 'repo', 42, 'linux-42', 1), ('linux', 'repo', 43, 'linux-43', 0);
		DROP TABLE runner_lifecycle;
		PRAGMA user_version = 3`); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, reopened)
	var provider, phase, reason string
	if err = reopened.database.QueryRow(`SELECT provider, phase, reap_reason FROM runner_lifecycle
		WHERE scale_set = 'linux' AND repository = 'repo' AND runner_name = 'linux-42'`).Scan(&provider, &phase, &reason); err != nil {
		t.Fatal(err)
	}
	if provider != "libvirt" || phase != "reap_pending" || reason != "legacy_reap_requested" {
		t.Fatalf("migrated lifecycle = provider %q phase %q reason %q", provider, phase, reason)
	}
	var waitingSince int64
	if err = reopened.database.QueryRow(`SELECT phase, waiting_since FROM runner_lifecycle
		WHERE scale_set = 'linux' AND repository = 'repo' AND runner_name = 'linux-43'`).Scan(&phase, &waitingSince); err != nil {
		t.Fatal(err)
	}
	if phase != "waiting_for_job" || waitingSince <= 0 {
		t.Fatalf("waiting migration = phase %q waiting_since %d", phase, waitingSince)
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
	if _, err := store.database.Exec("DROP TABLE runner_lifecycle; DROP TABLE libvirt_runner_lifecycle"); err != nil {
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
