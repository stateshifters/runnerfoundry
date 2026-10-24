package github

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestMultiAppCoordinatorResolvesOverlapByOrderAndIsolatesFailure(t *testing.T) {
	coordinator, first := overlapCoordinator(t)
	assertOverlapPrecedenceAndBootstrap(t, coordinator)
	first.discoverer = coordinatorDiscoverer{err: errors.New("first unavailable")}
	records, err := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if err == nil || len(records) != 3 {
		t.Fatalf("isolated App failure = routes %#v, error %v", records, err)
	}
	var degraded DegradedReconciliationError
	if !errors.As(err, &degraded) || degraded.Error() == "" || degraded.Unwrap() == nil || !degraded.RegistrationsAreUsable() {
		t.Fatalf("degraded reconciliation contract = %#v", err)
	}
	if appIDs := coordinator.GitHubAppIDs(); !reflect.DeepEqual(appIDs, []AppID{11, 22}) {
		t.Fatalf("configured App IDs = %#v", appIDs)
	}
}

func TestMultiAppCoordinatorTransfersRepositoryWhenHigherPriorityAppLosesVisibility(t *testing.T) {
	coordinator, first := overlapCoordinator(t)
	if _, err := coordinator.Reconcile(context.Background(), coordinatorManagerConfig()); err != nil {
		t.Fatalf("establish higher-priority route: %v", err)
	}
	first.discoverer = coordinatorDiscoverer{}
	records, err := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if err != nil || len(records) != 2 {
		t.Fatalf("same-pass route transfer = %#v, %v", records, err)
	}
	owners := make(map[string]AppID, len(records))
	for _, record := range records {
		owners[record.RepositoryURL] = record.GitHubAppID
	}
	if owners["https://github.com/example/shared"] != 22 {
		t.Fatalf("transferred route owners = %#v, want shared repository owned by App 22", owners)
	}
}

func TestMultiAppCoordinatorRetainsHigherPriorityClaimAfterPartialRepositoryFailure(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	first := coordinatorManager(t, store, 11, []transport.Repository{
		{InstallationID: 1, URL: "https://github.com/example/shared"},
		{InstallationID: 1, URL: "https://github.com/example/broken"},
	})
	first.factory = func(ctx context.Context, repository transport.Repository) (ManagerClient, error) {
		if repository.URL == "https://github.com/example/broken" {
			return ManagerClient{}, errors.New("repository unavailable")
		}
		return coordinatorClientFactory(11)(ctx, repository)
	}
	second := coordinatorManager(t, store, 22, []transport.Repository{{InstallationID: 2,
		URL: "https://github.com/example/shared"}})
	coordinator, err := NewMultiAppCoordinator(store, first, second)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	records, reconcileErr := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if reconcileErr == nil || len(records) != 1 || records[0].GitHubAppID != 11 {
		t.Fatalf("partial higher-priority result = %#v, %v", records, reconcileErr)
	}
}

func TestMultiAppCoordinatorReorderingChangesOverlapPrecedence(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	repository := []transport.Repository{{InstallationID: 1, URL: "https://github.com/example/shared"}}
	first := coordinatorManager(t, store, 11, repository)
	second := coordinatorManager(t, store, 22, repository)
	original, err := NewMultiAppCoordinator(store, first, second)
	if err != nil {
		t.Fatalf("create original coordinator: %v", err)
	}
	records, err := original.Reconcile(context.Background(), coordinatorManagerConfig())
	if err != nil || len(records) != 1 || records[0].GitHubAppID != 11 {
		t.Fatalf("original precedence = %#v, %v", records, err)
	}
	reordered, err := NewMultiAppCoordinator(store, second, first)
	if err != nil {
		t.Fatalf("create reordered coordinator: %v", err)
	}
	records, err = reordered.Reconcile(context.Background(), coordinatorManagerConfig())
	if err != nil || len(records) != 1 || records[0].GitHubAppID != 22 {
		t.Fatalf("reordered precedence = %#v, %v", records, err)
	}
}

func overlapCoordinator(t *testing.T) (*MultiAppCoordinator, *Manager) {
	t.Helper()
	store := &coordinatorRepositoryFake{}
	first := coordinatorManager(t, store, 11, []transport.Repository{
		{InstallationID: 1, URL: "https://github.com/example/shared"},
		{InstallationID: 1, URL: "https://github.com/example/first"},
	})
	second := coordinatorManager(t, store, 22, []transport.Repository{
		{InstallationID: 2, URL: "https://github.com/example/shared"},
		{InstallationID: 2, URL: "https://github.com/example/second"},
	})
	coordinator, err := NewMultiAppCoordinator(store, first, second)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	return coordinator, first
}

func assertOverlapPrecedenceAndBootstrap(t *testing.T, coordinator *MultiAppCoordinator) {
	t.Helper()
	records, err := coordinator.Reconcile(context.Background(), coordinatorManagerConfig())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	owners := make(map[string]AppID)
	for _, record := range records {
		owners[record.RepositoryURL] = record.GitHubAppID
	}
	if owners["https://github.com/example/shared"] != 11 || owners["https://github.com/example/second"] != 22 {
		t.Fatalf("route owners = %#v", owners)
	}
	if err = coordinator.RefreshMembers(context.Background(), coordinatorManagerConfig()); err != nil {
		t.Fatalf("refresh routes: %v", err)
	}
	assertCoordinatorBootstrap(t, coordinator, "https://github.com/example/first", "app-11")
	assertCoordinatorBootstrap(t, coordinator, "https://github.com/example/second", "app-22")
}

func assertCoordinatorBootstrap(t *testing.T, coordinator *MultiAppCoordinator, repository, expected string) {
	t.Helper()
	sourceID, err := EncodeSourceID(SourceIdentity{Repository: repository, RunnerPool: "linux", RequestID: 1})
	if err != nil {
		t.Fatalf("encode source ID: %v", err)
	}
	bootstrap, err := coordinator.Source().GenerateBootstrap(context.Background(), core.Job{
		Source: core.SourceReference{Adapter: AdapterName, ID: sourceID}, RunnerPool: "linux",
	}, "runner")
	if err != nil {
		t.Fatalf("generate bootstrap: %v", err)
	}
	if !reflect.DeepEqual(bootstrap, core.NewBootstrap([]byte(expected))) {
		t.Fatalf("bootstrap = %#v, want content %q", bootstrap, expected)
	}
}
