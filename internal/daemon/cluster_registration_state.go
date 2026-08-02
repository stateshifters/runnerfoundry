package daemon

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func (leader *clusterLeader) deletePersistedRegistration(
	ctx context.Context,
	key repositoryScaleSetSessionKey,
) error {
	if leader.daemon.repositoryState == nil || !leader.repositoryControlStarted {
		return nil
	}
	repositoryAppID, err := leader.activeRepositoryAppID()
	if err != nil {
		return err
	}
	err = leader.daemon.repositoryState.DeleteRegistration(
		ctx, repositoryAppID, leader.repositoryControllerEpoch,
		key.repository, key.scaleSet,
	)
	if err == nil {
		leader.registrationMutex.Lock()
		delete(leader.confirmedRegistrations, key)
		leader.registrationMutex.Unlock()
	}
	if err != nil {
		return fmt.Errorf("delete persisted registration %q: %w", key.scaleSet, err)
	}
	return nil
}

func (leader *clusterLeader) persistConfirmedRegistrations(
	ctx context.Context,
	repositoryURL string,
	statuses []cluster.ScaleSetStatus,
	registrations map[string]*upstream.RunnerScaleSet,
) error {
	if leader.daemon.repositoryState == nil || !leader.repositoryControlStarted {
		return nil
	}
	repositoryAppID, identityErr := leader.activeRepositoryAppID()
	if identityErr != nil {
		return identityErr
	}
	desiredNames := desiredRegistrationNames(statuses)
	upsertErr := leader.upsertConfirmedRegistrations(ctx, repositoryAppID, repositoryURL, statuses, registrations)
	deleteErr := leader.deleteStaleConfirmedRegistrations(ctx, repositoryURL, desiredNames)
	return errors.Join(upsertErr, deleteErr)
}

func desiredRegistrationNames(statuses []cluster.ScaleSetStatus) map[string]struct{} {
	desiredNames := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		desiredNames[status.Name] = struct{}{}
	}
	return desiredNames
}

func (leader *clusterLeader) upsertConfirmedRegistrations(
	ctx context.Context,
	repositoryAppID int64,
	repositoryURL string,
	statuses []cluster.ScaleSetStatus,
	registrations map[string]*upstream.RunnerScaleSet,
) error {
	var resultErr error
	for _, status := range statuses {
		registration := registrations[status.Name]
		if registration == nil {
			continue
		}
		err := leader.daemon.repositoryState.UpsertRegistration(
			ctx, repositoryAppID, leader.repositoryControllerEpoch,
			repositorystate.Registration{
				AppID: repositoryAppID, RepositoryURL: repositoryURL,
				ScaleSetName: status.Name, RunnerGroup: status.RunnerGroup, GitHubScaleSetID: registration.ID,
			},
		)
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("persist confirmed registration %q: %w", status.Name, err))
			continue
		}
		key := repositoryScaleSetSessionKey{repository: repositoryURL, scaleSet: status.Name}
		leader.registrationMutex.Lock()
		leader.confirmedRegistrations[key] = repositorystate.Registration{
			AppID: repositoryAppID, RepositoryURL: repositoryURL,
			ScaleSetName: status.Name, RunnerGroup: status.RunnerGroup, GitHubScaleSetID: registration.ID,
		}
		leader.registrationMutex.Unlock()
	}
	return resultErr
}

func (leader *clusterLeader) deleteStaleConfirmedRegistrations(
	ctx context.Context,
	repositoryURL string,
	desiredNames map[string]struct{},
) error {
	var resultErr error
	for _, staleKey := range leader.staleConfirmedRegistrationKeys(repositoryURL, desiredNames) {
		if err := leader.deletePersistedRegistration(ctx, staleKey); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("delete stale confirmed registration %q: %w", staleKey.scaleSet, err))
		}
	}
	return resultErr
}

func (leader *clusterLeader) staleConfirmedRegistrationKeys(
	repositoryURL string,
	desiredNames map[string]struct{},
) []repositoryScaleSetSessionKey {
	leader.registrationMutex.Lock()
	defer leader.registrationMutex.Unlock()
	stale := make([]repositoryScaleSetSessionKey, 0)
	for key := range leader.confirmedRegistrations {
		_, desired := desiredNames[key.scaleSet]
		if key.repository == repositoryURL && !desired {
			stale = append(stale, key)
		}
	}
	return stale
}
