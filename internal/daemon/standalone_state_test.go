package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestMemoryStandaloneStateRetainsFailedInstallationAndRemovesWithdrawnState(t *testing.T) {
	state := newMemoryStandaloneState()
	snapshot := mustStartMemoryController(t, state, 7)
	completedAt := time.Unix(100, 0)
	snapshot = mustApplyMemoryDiscovery(t, state, 7, snapshot.Control.ControllerEpoch, nil, []repositorystate.Repository{
		{AppID: 7, URL: "retained", InstallationID: 1},
		{AppID: 7, URL: "withdrawn", InstallationID: 2},
	}, completedAt)
	mustUpsertMemoryRegistration(t, state, 7, snapshot.Control.ControllerEpoch, repositorystate.Registration{
		AppID: 7, RepositoryURL: "withdrawn", ScaleSetName: "linux", GitHubScaleSetID: 1,
	})
	mustUpsertMemoryRegistration(t, state, 7, snapshot.Control.ControllerEpoch, repositorystate.Registration{
		AppID: 7, RepositoryURL: "retained", ScaleSetName: "linux", GitHubScaleSetID: 2,
	})
	snapshot = mustApplyMemoryDiscovery(t, state, 7, snapshot.Control.ControllerEpoch,
		[]githubapp.InstallationID{1}, []repositorystate.Repository{{
			AppID: 7, URL: "new", InstallationID: 3,
		}}, completedAt.Add(time.Minute))
	assertRetainedDiscoverySnapshot(t, snapshot, completedAt.Add(time.Minute))
}

func assertRetainedDiscoverySnapshot(t *testing.T, snapshot repositorystate.Snapshot, completedAt time.Time) {
	t.Helper()
	if len(snapshot.Repositories) != 2 || len(snapshot.Registrations) != 1 ||
		snapshot.Control.DiscoveryGeneration != 2 || !snapshot.Control.LastSuccessfulDiscovery.Equal(completedAt) {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	repositories := map[string]repositorystate.Repository{}
	for _, repository := range snapshot.Repositories {
		repositories[repository.URL] = repository
	}
	if _, retained := repositories["retained"]; !retained || repositories["new"].LastSeenGeneration != 2 {
		t.Fatalf("repositories = %#v", repositories)
	}
}

func TestMemoryStandaloneStateReplacesAndDeletesRegistration(t *testing.T) {
	ctx := context.Background()
	state := newMemoryStandaloneState()
	snapshot := mustStartMemoryController(t, state, 1)
	registration := repositorystate.Registration{
		AppID: 1, RepositoryURL: "repo", ScaleSetName: "linux", GitHubScaleSetID: 1,
	}
	mustUpsertMemoryRegistration(t, state, 1, snapshot.Control.ControllerEpoch, registration)
	registration.GitHubScaleSetID = 2
	mustUpsertMemoryRegistration(t, state, 1, snapshot.Control.ControllerEpoch, registration)
	mustUpsertMemoryRegistration(t, state, 1, snapshot.Control.ControllerEpoch, repositorystate.Registration{
		AppID: 1, RepositoryURL: "repo", ScaleSetName: "windows", GitHubScaleSetID: 3,
	})
	if err := state.DeleteRegistration(ctx, 1, snapshot.Control.ControllerEpoch, "repo", "linux"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := state.LoadSnapshot(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Registrations) != 1 || snapshot.Registrations[0].ScaleSetName != "windows" {
		t.Fatalf("registrations = %#v", snapshot.Registrations)
	}
}

func mustStartMemoryController(t *testing.T, state *memoryStandaloneState, appID int64) repositorystate.Snapshot {
	t.Helper()
	snapshot, err := state.StartController(context.Background(), appID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func mustApplyMemoryDiscovery(
	t *testing.T,
	state *memoryStandaloneState,
	appID int64,
	epoch int64,
	failed []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) repositorystate.Snapshot {
	t.Helper()
	snapshot, err := state.ApplyDiscovery(
		context.Background(), appID, epoch, nil, failed, repositories, completedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func mustUpsertMemoryRegistration(
	t *testing.T,
	state *memoryStandaloneState,
	appID int64,
	epoch int64,
	registration repositorystate.Registration,
) {
	t.Helper()
	if err := state.UpsertRegistration(context.Background(), appID, epoch, registration); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStandaloneStateRejectsFencedMutations(t *testing.T) {
	ctx := context.Background()
	state := newMemoryStandaloneState()
	if _, err := state.StartController(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyDiscovery(ctx, 1, 0, nil, nil, nil, time.Time{}); !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("apply error = %v", err)
	}
	if err := state.UpsertRegistration(ctx, 1, 0, repositorystate.Registration{}); !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("upsert error = %v", err)
	}
	if err := state.DeleteRegistration(ctx, 1, 0, "repo", "linux"); !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("delete error = %v", err)
	}
}
