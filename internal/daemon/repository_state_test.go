package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

type fakeRepositoryStateStore struct {
	mutex    sync.Mutex
	snapshot repositorystate.Snapshot
}

func (store *fakeRepositoryStateStore) StartController(
	_ context.Context,
	appID int64,
) (repositorystate.Snapshot, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.snapshot.Control.AppID = appID
	store.snapshot.Control.ControllerEpoch++
	return store.snapshot, nil
}

func (store *fakeRepositoryStateStore) LoadSnapshot(
	_ context.Context,
	appID int64,
) (repositorystate.Snapshot, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	snapshot := store.snapshot
	snapshot.Control.AppID = appID
	return snapshot, nil
}

func (store *fakeRepositoryStateStore) ApplyDiscovery(
	_ context.Context,
	appID int64,
	controllerEpoch int64,
	installationIDs []githubapp.InstallationID,
	failedInstallationIDs []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) (repositorystate.Snapshot, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.snapshot.Control.ControllerEpoch != controllerEpoch {
		return repositorystate.Snapshot{}, repositorystate.ErrControllerFenced
	}
	store.snapshot.Control.AppID = appID
	store.snapshot.Control.DiscoveryGeneration++
	store.snapshot.Control.LastSuccessfulDiscovery = completedAt
	store.snapshot.Control.Initialized = true
	store.snapshot.Repositories = mergeFakeDiscoveredRepositories(
		store.snapshot.Repositories, installationIDs, failedInstallationIDs, repositories,
	)
	return store.snapshot, nil
}

func (store *fakeRepositoryStateStore) UpsertRegistration(
	_ context.Context,
	appID int64,
	controllerEpoch int64,
	registration repositorystate.Registration,
) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.snapshot.Control.ControllerEpoch != controllerEpoch {
		return repositorystate.ErrControllerFenced
	}
	registration.AppID = appID
	for index, existing := range store.snapshot.Registrations {
		if existing.RepositoryURL == registration.RepositoryURL && existing.ScaleSetName == registration.ScaleSetName {
			store.snapshot.Registrations[index] = registration
			return nil
		}
	}
	store.snapshot.Registrations = append(store.snapshot.Registrations, registration)
	return nil
}

func (store *fakeRepositoryStateStore) DeleteRegistration(
	_ context.Context,
	_ int64,
	controllerEpoch int64,
	repositoryURL string,
	scaleSetName string,
) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.snapshot.Control.ControllerEpoch != controllerEpoch {
		return repositorystate.ErrControllerFenced
	}
	registrations := store.snapshot.Registrations[:0]
	for _, registration := range store.snapshot.Registrations {
		if registration.RepositoryURL != repositoryURL || registration.ScaleSetName != scaleSetName {
			registrations = append(registrations, registration)
		}
	}
	store.snapshot.Registrations = registrations
	return nil
}

func mergeFakeDiscoveredRepositories(
	existing []repositorystate.Repository,
	installationIDs []githubapp.InstallationID,
	failedInstallationIDs []githubapp.InstallationID,
	discovered []repositorystate.Repository,
) []repositorystate.Repository {
	failed := make(map[githubapp.InstallationID]struct{}, len(failedInstallationIDs))
	for _, installationID := range failedInstallationIDs {
		failed[installationID] = struct{}{}
	}
	result := append([]repositorystate.Repository(nil), discovered...)
	for _, repository := range existing {
		if _, retain := failed[repository.InstallationID]; retain {
			result = append(result, repository)
		}
	}
	_ = installationIDs
	return result
}
