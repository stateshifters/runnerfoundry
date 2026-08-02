package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type memoryStandaloneState struct {
	scaler.DemandStore
	mutex     sync.Mutex
	snapshots map[int64]repositorystate.Snapshot
}

type standaloneStateReference struct {
	scaler.DemandStore
	repositorystate.Store
}

func (standaloneStateReference) Close() error { return nil }

func newMemoryStandaloneState() *memoryStandaloneState {
	return &memoryStandaloneState{
		DemandStore: scaler.NewMemoryDemandStore(), snapshots: map[int64]repositorystate.Snapshot{},
	}
}

func (*memoryStandaloneState) Close() error { return nil }

func (state *memoryStandaloneState) StartController(_ context.Context, appID int64) (repositorystate.Snapshot, error) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	snapshot := state.snapshots[appID]
	snapshot.Control.AppID = appID
	snapshot.Control.ControllerEpoch++
	snapshot.Control.Initialized = true
	state.snapshots[appID] = snapshot
	return snapshot, nil
}

func (state *memoryStandaloneState) LoadSnapshot(_ context.Context, appID int64) (repositorystate.Snapshot, error) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	return state.snapshots[appID], nil
}

func (state *memoryStandaloneState) ApplyDiscovery(
	_ context.Context,
	appID int64,
	controllerEpoch int64,
	_ []githubapp.InstallationID,
	failedInstallationIDs []githubapp.InstallationID,
	repositories []repositorystate.Repository,
	completedAt time.Time,
) (repositorystate.Snapshot, error) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	snapshot := state.snapshots[appID]
	if snapshot.Control.ControllerEpoch != controllerEpoch {
		return repositorystate.Snapshot{}, repositorystate.ErrControllerFenced
	}
	failed := failedInstallationSet(failedInstallationIDs)
	retained := retainedStandaloneRepositories(snapshot.Repositories, repositories, failed,
		snapshot.Control.DiscoveryGeneration+1)
	snapshot.Repositories = repositoryValues(retained)
	snapshot.Registrations = retainedRegistrations(snapshot.Registrations, retained)
	snapshot.Control.DiscoveryGeneration++
	snapshot.Control.LastSuccessfulDiscovery = completedAt
	state.snapshots[appID] = snapshot
	return snapshot, nil
}

func failedInstallationSet(installationIDs []githubapp.InstallationID) map[githubapp.InstallationID]struct{} {
	failed := make(map[githubapp.InstallationID]struct{}, len(installationIDs))
	for _, installationID := range installationIDs {
		failed[installationID] = struct{}{}
	}
	return failed
}

func retainedStandaloneRepositories(
	previous []repositorystate.Repository,
	discovered []repositorystate.Repository,
	failed map[githubapp.InstallationID]struct{},
	generation int64,
) map[string]repositorystate.Repository {
	retained := make(map[string]repositorystate.Repository, len(previous)+len(discovered))
	for _, repository := range previous {
		if _, keep := failed[repository.InstallationID]; keep {
			retained[repository.URL] = repository
		}
	}
	for _, repository := range discovered {
		repository.LastSeenGeneration = generation
		retained[repository.URL] = repository
	}
	return retained
}

func repositoryValues(repositories map[string]repositorystate.Repository) []repositorystate.Repository {
	values := make([]repositorystate.Repository, 0, len(repositories))
	for _, repository := range repositories {
		values = append(values, repository)
	}
	return values
}

func retainedRegistrations(
	registrations []repositorystate.Registration,
	repositories map[string]repositorystate.Repository,
) []repositorystate.Registration {
	retained := registrations[:0]
	for _, registration := range registrations {
		if _, repositoryRetained := repositories[registration.RepositoryURL]; repositoryRetained {
			retained = append(retained, registration)
		}
	}
	return retained
}

func (state *memoryStandaloneState) UpsertRegistration(
	_ context.Context, appID int64, controllerEpoch int64, registration repositorystate.Registration,
) error {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	snapshot := state.snapshots[appID]
	if snapshot.Control.ControllerEpoch != controllerEpoch {
		return repositorystate.ErrControllerFenced
	}
	for index, existing := range snapshot.Registrations {
		if existing.RepositoryURL == registration.RepositoryURL && existing.ScaleSetName == registration.ScaleSetName {
			snapshot.Registrations[index] = registration
			state.snapshots[appID] = snapshot
			return nil
		}
	}
	snapshot.Registrations = append(snapshot.Registrations, registration)
	state.snapshots[appID] = snapshot
	return nil
}

func (state *memoryStandaloneState) DeleteRegistration(
	_ context.Context, appID int64, controllerEpoch int64, repositoryURL string, scaleSetName string,
) error {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	snapshot := state.snapshots[appID]
	if snapshot.Control.ControllerEpoch != controllerEpoch {
		return repositorystate.ErrControllerFenced
	}
	registrations := snapshot.Registrations[:0]
	for _, registration := range snapshot.Registrations {
		if registration.RepositoryURL != repositoryURL || registration.ScaleSetName != scaleSetName {
			registrations = append(registrations, registration)
		}
	}
	snapshot.Registrations = registrations
	state.snapshots[appID] = snapshot
	return nil
}
