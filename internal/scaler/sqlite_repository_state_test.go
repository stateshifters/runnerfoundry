package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestSQLiteRepositoryStateRetainsFailedInstallations(t *testing.T) {
	store := newSQLiteRepositoryStateStore(t)
	ctx := context.Background()
	started, err := store.StartController(ctx, 7)
	if err != nil || started.Control.ControllerEpoch != 1 || started.Control.Initialized {
		t.Fatalf("started snapshot = %#v, %v", started, err)
	}
	completedAt := time.Unix(100, 200).UTC()
	first, err := store.ApplyDiscovery(ctx, 7, 1, []githubapp.InstallationID{11, 12}, nil, []repositorystate.Repository{
		{URL: "repo/retained", InstallationID: 11},
		{URL: "repo/omitted", InstallationID: 11},
		{URL: "repo/failed-installation", InstallationID: 12},
	}, completedAt)
	if err != nil || first.Control.DiscoveryGeneration != 1 || !first.Control.Initialized {
		t.Fatalf("first discovery = %#v, %v", first, err)
	}
	registerSQLiteRepositories(t, store)
	second, err := store.ApplyDiscovery(ctx, 7, 1, []githubapp.InstallationID{11, 12}, []githubapp.InstallationID{12},
		[]repositorystate.Repository{{URL: "repo/retained", InstallationID: 11}}, completedAt.Add(time.Minute))
	if err != nil || len(second.Repositories) != 2 || len(second.Registrations) != 2 {
		t.Fatalf("second snapshot = %#v, %v", second, err)
	}
	loaded, err := store.LoadSnapshot(ctx, 7)
	if err != nil || len(loaded.Repositories) != 2 || len(loaded.Registrations) != 2 {
		t.Fatalf("loaded snapshot = %#v, %v", loaded, err)
	}
}

func TestSQLiteRepositoryStateFencesControllersAndLoadsEmptyState(t *testing.T) {
	store := newSQLiteRepositoryStateStore(t)
	ctx := context.Background()
	if _, err := store.StartController(ctx, 7); err != nil {
		t.Fatal(err)
	}
	newController, err := store.StartController(ctx, 7)
	if err != nil || newController.Control.ControllerEpoch != 2 {
		t.Fatalf("new controller = %#v, %v", newController.Control, err)
	}
	if err = store.DeleteRegistration(ctx, 7, 2, "repo/retained", "linux"); err != nil {
		t.Fatal(err)
	}
	err = store.DeleteRegistration(ctx, 7, 1, "repo/retained", "linux")
	if !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("stale controller error = %v", err)
	}
	empty, err := store.LoadSnapshot(ctx, 99)
	if err != nil || empty.Control.AppID != 99 {
		t.Fatalf("empty snapshot = %#v, %v", empty, err)
	}
}

func TestSQLiteRepositoryStateRejectsInvalidRows(t *testing.T) {
	store := newSQLiteRepositoryStateStore(t)
	ctx := context.Background()
	if _, err := store.StartController(ctx, 7); err != nil {
		t.Fatal(err)
	}
	completedAt := time.Unix(100, 200).UTC()
	if _, err := store.ApplyDiscovery(ctx, 7, 1, []githubapp.InstallationID{0}, nil, nil, completedAt); err == nil {
		t.Fatal("expected invalid installation failure")
	}
	if _, err := store.ApplyDiscovery(ctx, 7, 1, []githubapp.InstallationID{11}, nil,
		[]repositorystate.Repository{{URL: "", InstallationID: 11}}, completedAt); err == nil {
		t.Fatal("expected invalid repository failure")
	}
	if err := store.UpsertRegistration(ctx, 7, 1, repositorystate.Registration{
		RepositoryURL: "repo/retained", ScaleSetName: "invalid", RunnerGroup: "Default",
	}); err == nil {
		t.Fatal("expected invalid registration failure")
	}
}

func TestSQLiteRepositoryStateRejectsOperationsAfterClose(t *testing.T) {
	store := openSQLiteRepositoryStateStore(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartController(context.Background(), 7); err == nil {
		t.Fatal("expected closed database failure")
	}
}

func registerSQLiteRepositories(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	for _, registration := range []repositorystate.Registration{
		{RepositoryURL: "repo/retained", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 21},
		{RepositoryURL: "repo/omitted", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 22},
		{RepositoryURL: "repo/failed-installation", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 23},
	} {
		if err := store.UpsertRegistration(context.Background(), 7, 1, registration); err != nil {
			t.Fatal(err)
		}
	}
}

func newSQLiteRepositoryStateStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	store := openSQLiteRepositoryStateStore(t)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close repository state store: %v", err)
		}
	})
	return store
}

func openSQLiteRepositoryStateStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store, err := createSQLiteDemandStore("state.db", sqliteStoreDependencies{
		newLock:      func(string) stateDatabaseLock { return &fakeStateDatabaseLock{locked: true} },
		openDatabase: func(string) (*sql.DB, error) { return database, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
