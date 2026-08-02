package repositorystate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestStoreContractCarriesDiscoveryAndRegistrationState(t *testing.T) {
	var _ Store = contractStore{}
	snapshot := Snapshot{
		Control:       Control{AppID: 1, DiscoveryGeneration: 2, LastSuccessfulDiscovery: time.Unix(3, 0), ControllerEpoch: 4, Initialized: true},
		Repositories:  []Repository{{AppID: 1, URL: "repo", InstallationID: githubapp.InstallationID(5), LastSeenGeneration: 2}},
		Registrations: []Registration{{AppID: 1, RepositoryURL: "repo", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 6}},
	}
	if snapshot.Repositories[0].InstallationID != 5 || snapshot.Registrations[0].GitHubScaleSetID != 6 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if !errors.Is(ErrControllerFenced, ErrControllerFenced) {
		t.Fatal("controller fencing sentinel is unavailable")
	}
}

type contractStore struct{}

func (contractStore) StartController(context.Context, int64) (Snapshot, error) {
	return Snapshot{}, nil
}
func (contractStore) LoadSnapshot(context.Context, int64) (Snapshot, error) { return Snapshot{}, nil }
func (contractStore) ApplyDiscovery(context.Context, int64, int64, []githubapp.InstallationID, []githubapp.InstallationID, []Repository, time.Time) (Snapshot, error) {
	return Snapshot{}, nil
}
func (contractStore) UpsertRegistration(context.Context, int64, int64, Registration) error {
	return nil
}
func (contractStore) DeleteRegistration(context.Context, int64, int64, string, string) error {
	return nil
}
