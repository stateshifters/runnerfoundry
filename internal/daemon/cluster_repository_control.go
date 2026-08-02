package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

type repositoryDiscoveryOutcome struct {
	discovery githubapp.DiscoveryResult
	err       error
}

func (leader *clusterLeader) reconcileDurableRepositories(ctx context.Context, termCtx context.Context) bool {
	if !leader.startRepositoryController(ctx) {
		return false
	}
	discoveryResults := leader.beginRepositoryDiscovery(termCtx)
	statuses, loaded := leader.loadClusterScaleSetStatuses(ctx)
	if !loaded {
		leader.continueStartedRepositoryDiscovery(termCtx, discoveryResults, nil, nil)
		return false
	}
	cachedRepositories := repositorySnapshotRepositories(leader.repositorySnapshot)
	if leader.repositorySnapshot.Control.Initialized {
		return leader.reconcileCachedRepositories(
			ctx, termCtx, cachedRepositories, statuses, discoveryResults,
		)
	}
	return leader.reconcileFreshRepositories(ctx, discoveryResults, statuses)
}

func (leader *clusterLeader) continueStartedRepositoryDiscovery(
	ctx context.Context,
	results <-chan repositoryDiscoveryOutcome,
	statuses []cluster.ScaleSetStatus,
	cached []githubapp.Repository,
) {
	if results != nil {
		leader.continueRepositoryDiscovery(ctx, results, statuses, cached)
	}
}

func (leader *clusterLeader) reconcileCachedRepositories(
	ctx context.Context,
	termCtx context.Context,
	cached []githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
	discoveryResults <-chan repositoryDiscoveryOutcome,
) bool {
	leader.reconcileRepositorySet(termCtx, cached, statuses)
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_catalog", "loaded",
		slog.Int("repositoryCount", len(cached)),
		slog.Int64("generation", leader.repositorySnapshot.Control.DiscoveryGeneration),
		slog.Int64("epoch", leader.repositorySnapshot.Control.ControllerEpoch))
	leader.continueStartedRepositoryDiscovery(termCtx, discoveryResults, statuses, cached)
	return ctx.Err() == nil
}

func (leader *clusterLeader) reconcileFreshRepositories(
	ctx context.Context,
	discoveryResults <-chan repositoryDiscoveryOutcome,
	statuses []cluster.ScaleSetStatus,
) bool {
	if discoveryResults == nil {
		return false
	}
	outcome := <-discoveryResults
	leader.finishRepositoryDiscovery()
	if outcome.err != nil {
		leader.daemon.logger.Error("leader repository discovery failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", outcome.err.Error()))
		return false
	}
	freshSnapshot, applied := leader.applyRepositoryDiscovery(ctx, outcome.discovery)
	if !applied {
		return false
	}
	leader.repositorySnapshot = freshSnapshot
	freshRepositories := repositorySnapshotRepositories(freshSnapshot)
	leader.reconcileRepositorySet(ctx, freshRepositories, statuses)
	return ctx.Err() == nil
}

func (leader *clusterLeader) beginRepositoryDiscovery(ctx context.Context) <-chan repositoryDiscoveryOutcome {
	leader.discoveryMutex.Lock()
	defer leader.discoveryMutex.Unlock()
	if leader.discoveryRunning || ctx.Err() != nil {
		return nil
	}
	leader.discoveryRunning = true
	results := make(chan repositoryDiscoveryOutcome, 1)
	go leader.discoverRepositories(ctx, results)
	return results
}

func (leader *clusterLeader) continueRepositoryDiscovery(
	ctx context.Context,
	results <-chan repositoryDiscoveryOutcome,
	statuses []cluster.ScaleSetStatus,
	cachedRepositories []githubapp.Repository,
) {
	leader.background.Add(1)
	go func() {
		defer leader.background.Done()
		outcome := <-results
		leader.operationMutex.Lock()
		defer leader.operationMutex.Unlock()
		defer leader.finishRepositoryDiscovery()
		if outcome.err != nil {
			logAction(ctx, leader.daemon.logger, slog.LevelWarn, "discovery", "stale",
				slog.String("error", outcome.err.Error()),
				slog.Int64("generation", leader.repositorySnapshot.Control.DiscoveryGeneration))
			return
		}
		freshSnapshot, applied := leader.applyRepositoryDiscovery(ctx, outcome.discovery)
		if !applied {
			return
		}
		leader.repositorySnapshot = freshSnapshot
		freshRepositories := repositorySnapshotRepositories(freshSnapshot)
		if statuses != nil && !sameRepositorySet(cachedRepositories, freshRepositories) {
			leader.reconcileRepositorySet(ctx, freshRepositories, statuses)
		}
	}()
}

func (leader *clusterLeader) finishRepositoryDiscovery() {
	leader.discoveryMutex.Lock()
	leader.discoveryRunning = false
	leader.discoveryMutex.Unlock()
}

func (leader *clusterLeader) startRepositoryController(ctx context.Context) bool {
	if leader.repositoryControlStarted {
		if _, err := leader.activeRepositoryAppID(); err != nil {
			leader.daemon.logger.Error("repository controller fenced", slog.String("error", err.Error()))
			return false
		}
		return true
	}
	configuration := leader.daemon.configurationSnapshot()
	repositoryAppID := configuration.credentials.AppID
	controllerCtx, cancelController := context.WithTimeout(ctx, clusterCoordinationTimeout)
	snapshot, err := leader.daemon.repositoryState.StartController(controllerCtx, repositoryAppID)
	cancelController()
	if err != nil {
		leader.daemon.logger.Error("start repository controller failed", slog.String("error", err.Error()))
		return false
	}
	leader.repositorySnapshot = snapshot
	leader.repositoryAppID = repositoryAppID
	leader.repositoryControllerEpoch = snapshot.Control.ControllerEpoch
	leader.registrationMutex.Lock()
	for _, registration := range snapshot.Registrations {
		key := repositoryScaleSetSessionKey{repository: registration.RepositoryURL, scaleSet: registration.ScaleSetName}
		leader.confirmedRegistrations[key] = registration
	}
	leader.registrationMutex.Unlock()
	leader.repositoryControlStarted = true
	return true
}

func (leader *clusterLeader) discoverRepositories(ctx context.Context, results chan<- repositoryDiscoveryOutcome) {
	configuration := leader.daemon.configurationSnapshot()
	discoveryCtx, cancelDiscovery := context.WithTimeout(ctx, leader.githubOperationTimeout)
	discovery, err := configuration.discoverer.Discover(discoveryCtx)
	cancelDiscovery()
	results <- repositoryDiscoveryOutcome{discovery: discovery, err: err}
}

func (leader *clusterLeader) loadClusterScaleSetStatuses(ctx context.Context) ([]cluster.ScaleSetStatus, bool) {
	statusCtx, cancelStatus := context.WithTimeout(ctx, clusterCoordinationTimeout)
	statuses, err := leader.daemon.nodes.ScaleSets(statusCtx, time.Now(), clusterNodeLifetime)
	cancelStatus()
	if err != nil {
		leader.daemon.logger.Error("leader scale-set aggregation failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
		return nil, false
	}
	return statuses, true
}

func (leader *clusterLeader) applyRepositoryDiscovery(
	ctx context.Context,
	discovery githubapp.DiscoveryResult,
) (repositorystate.Snapshot, bool) {
	repositoryAppID, identityErr := leader.activeRepositoryAppID()
	if identityErr != nil {
		leader.daemon.logger.Error("publish repository discovery failed", slog.String("error", identityErr.Error()))
		return repositorystate.Snapshot{}, false
	}
	repositories := durableDiscoveryRepositories(repositoryAppID, discovery.Repositories)
	failedInstallations := failedDiscoveryInstallationIDs(discovery.InstallationFailures)
	return leader.publishRepositoryDiscovery(ctx, repositoryAppID, discovery, failedInstallations, repositories)
}

func durableDiscoveryRepositories(
	repositoryAppID int64,
	discovered []githubapp.Repository,
) []repositorystate.Repository {
	repositories := make([]repositorystate.Repository, 0, len(discovered))
	for _, repository := range discovered {
		repositories = append(repositories, repositorystate.Repository{
			AppID: repositoryAppID, URL: repository.URL, InstallationID: repository.InstallationID,
		})
	}
	return repositories
}

func failedDiscoveryInstallationIDs(
	failures map[githubapp.InstallationID]error,
) []githubapp.InstallationID {
	failedInstallations := make([]githubapp.InstallationID, 0, len(failures))
	for installationID := range failures {
		failedInstallations = append(failedInstallations, installationID)
	}
	return failedInstallations
}

func (leader *clusterLeader) publishRepositoryDiscovery(
	ctx context.Context,
	repositoryAppID int64,
	discovery githubapp.DiscoveryResult,
	failedInstallations []githubapp.InstallationID,
	repositories []repositorystate.Repository,
) (repositorystate.Snapshot, bool) {
	applyCtx, cancelApply := context.WithTimeout(ctx, clusterCoordinationTimeout)
	snapshot, err := leader.daemon.repositoryState.ApplyDiscovery(
		applyCtx, repositoryAppID, leader.repositoryControllerEpoch,
		discovery.Installations, failedInstallations, repositories, time.Now().UTC(),
	)
	cancelApply()
	if err != nil {
		leader.daemon.logger.Error("publish repository discovery failed", slog.String("error", err.Error()))
		return repositorystate.Snapshot{}, false
	}
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_catalog", "applied",
		slog.Int("repositoryCount", len(snapshot.Repositories)),
		slog.Int64("generation", snapshot.Control.DiscoveryGeneration),
		slog.Int64("epoch", snapshot.Control.ControllerEpoch))
	return snapshot, true
}

func (leader *clusterLeader) activeRepositoryAppID() (int64, error) {
	configuration := leader.daemon.configurationSnapshot()
	if configuration.credentials.AppID != leader.repositoryAppID {
		return 0, fmt.Errorf(
			"repository controller app changed from %d to %d: %w",
			leader.repositoryAppID, configuration.credentials.AppID, repositorystate.ErrControllerFenced,
		)
	}
	return leader.repositoryAppID, nil
}

func (leader *clusterLeader) reconcileRepositorySet(
	ctx context.Context,
	repositories []githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) {
	desired, repositoryMap := leader.desiredSessions(repositories, statuses)
	leader.publishDesiredRepositories(repositoryMap, desired)
	leader.stopUndesiredSessions(ctx, desired, statuses)
	leader.retainDesiredRepositoryRepairs(repositoryMap, desired)
	leader.scheduleMissingSessions(ctx, desired, repositoryMap)
}

func (leader *clusterLeader) scheduleMissingSessions(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	repositories map[string]githubapp.Repository,
) {
	for repositoryURL, repository := range repositories {
		statuses := desiredRepositoryStatuses(repositoryURL, desired)
		if len(statuses) == 0 {
			continue
		}
		leader.repositoryWorkers.enqueue(repositoryURL, repositoryWorkRoutine, func(attemptCtx context.Context) error {
			return leader.reconcileDesiredRepository(attemptCtx, repository, statuses)
		}, func(err error) {
			if err == nil || errors.Is(err, errRepositoryWorkObsolete) {
				leader.completeRepositoryRepair(ctx, repository.URL)
				return
			}
			leader.daemon.logger.Error("leader repository session group failed",
				slog.String("repository", repository.URL), slog.String("error", err.Error()))
			leader.recordRepositoryRepair(ctx, repository, statuses, err)
		})
	}
}

func (leader *clusterLeader) publishDesiredRepositories(
	repositories map[string]githubapp.Repository,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
) {
	leader.desiredMutex.Lock()
	defer leader.desiredMutex.Unlock()
	leader.desiredRepositories = make(map[string][]cluster.ScaleSetStatus, len(repositories))
	for repositoryURL := range repositories {
		leader.desiredRepositories[repositoryURL] = desiredRepositoryStatuses(repositoryURL, desired)
	}
	leader.desiredStateKnown = true
}

func repositorySnapshotRepositories(snapshot repositorystate.Snapshot) []githubapp.Repository {
	repositories := make([]githubapp.Repository, 0, len(snapshot.Repositories))
	for _, repository := range snapshot.Repositories {
		repositories = append(repositories, githubapp.Repository{
			InstallationID: repository.InstallationID, URL: repository.URL,
		})
	}
	return repositories
}

func sameRepositorySet(left []githubapp.Repository, right []githubapp.Repository) bool {
	if len(left) != len(right) {
		return false
	}
	leftByURL := make(map[string]githubapp.InstallationID, len(left))
	for _, repository := range left {
		leftByURL[repository.URL] = repository.InstallationID
	}
	for _, repository := range right {
		if leftByURL[repository.URL] != repository.InstallationID {
			return false
		}
	}
	return true
}
