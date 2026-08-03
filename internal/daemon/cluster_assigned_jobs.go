package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const assignedJobRecoveryInterval = 5 * time.Second

func (leader *clusterLeader) activateRepositorySessionGeneration(
	ctx context.Context,
	repository string,
	statuses []cluster.ScaleSetStatus,
	started []*repositoryScaleSetSession,
) error {
	desired, exists := leader.currentDesiredRepository(repository)
	if !exists {
		desired = desiredRepositoryGeneration{repository: githubapp.Repository{URL: repository}, statuses: statuses}
	}
	return leader.activateDesiredRepositorySessionGeneration(ctx, desired, started)
}

func (leader *clusterLeader) activateDesiredRepositorySessionGeneration(
	ctx context.Context,
	expected desiredRepositoryGeneration,
	started []*repositoryScaleSetSession,
) error {
	if err := leader.bootstrapRepositorySessionGeneration(ctx, started); err != nil {
		return err
	}
	leader.desiredMutex.Lock()
	desired, exists := leader.desiredRepositories[expected.repository.URL]
	if leader.desiredStateKnown && (!exists || desired.generation != expected.generation ||
		desired.repository != expected.repository || !sameScaleSetStatuses(desired.statuses, expected.statuses)) {
		leader.desiredMutex.Unlock()
		return errRepositoryWorkObsolete
	}
	leader.desiredMutex.Unlock()
	leader.sessionsMutex.Lock()
	for _, session := range started {
		leader.sessions[session.key] = session
	}
	leader.sessionsMutex.Unlock()
	for _, session := range started {
		leader.scheduler.add(session)
	}
	for _, session := range started {
		leader.startSessionRuntime(ctx, session)
	}
	return nil
}

func (leader *clusterLeader) bootstrapRepositorySessionGeneration(
	ctx context.Context,
	sessions []*repositoryScaleSetSession,
) error {
	for _, session := range sessions {
		if err := session.assignedJobs.prepareInitial(ctx, session.messageClient.initialStatistics()); err != nil {
			return fmt.Errorf("bootstrap assigned jobs for %s/%s: %w", session.key.repository, session.key.scaleSet, err)
		}
	}
	return nil
}

func (leader *clusterLeader) prepareSessionRuntime(session *repositoryScaleSetSession) {
	stopped := make(chan struct{})
	close(stopped)
	session.stopPolling = func() {}
	session.pollerDone = stopped
	session.assignedJobs = newClusterAssignedJobRecovery(leader, session)
}

func (leader *clusterLeader) startSessionRuntime(ctx context.Context, session *repositoryScaleSetSession) {
	runtimeCtx, stopRuntime := leader.newTermContext(ctx)
	session.stopPolling = stopRuntime
	session.assignedJobs.start(runtimeCtx)
	session.pollerDone = leader.startSessionPoller(runtimeCtx, session)
}

func (leader *clusterLeader) wakeAssignedJobRecovery(ctx context.Context) {
	if ctx.Err() != nil || leader.scheduler == nil {
		return
	}
	leader.sessionsMutex.Lock()
	scaleSets := make(map[string]struct{}, len(leader.sessions))
	for key := range leader.sessions {
		scaleSets[key.scaleSet] = struct{}{}
	}
	leader.sessionsMutex.Unlock()
	for scaleSet := range scaleSets {
		leader.scheduler.wake(ctx, scaleSet)
	}
}

type clusterAssignedJobBackend struct {
	state            clusterAssignedJobState
	appID            int64
	repository       string
	scaleSet         string
	controllerEpoch  int64
	now              func() time.Time
	assignedJobs     int
	latestResult     cluster.AssignedJobObservationResult
	latestObservedAt time.Time
}

func (backend clusterAssignedJobBackend) observation(assignedJobs int) cluster.AssignedJobObservation {
	return cluster.AssignedJobObservation{
		AppID: backend.appID, Repository: backend.repository, ScaleSet: backend.scaleSet,
		ControllerEpoch: backend.controllerEpoch, AssignedJobs: assignedJobs, ObservedAt: backend.now().UTC(),
	}
}

func (backend *clusterAssignedJobBackend) Prepare(
	ctx context.Context,
	assignedJobs int,
) (scaler.AssignedJobCapacity, error) {
	backend.assignedJobs = assignedJobs
	observation := backend.observation(assignedJobs)
	result, err := backend.state.ObserveAssignedJobs(ctx, observation)
	if err != nil {
		return scaler.AssignedJobCapacity{}, fmt.Errorf("observe clustered assigned jobs: %w", err)
	}
	backend.latestResult = result
	backend.latestObservedAt = observation.ObservedAt
	return result.Capacity, nil
}

type clusterAssignedJobRecovery struct {
	backend          *clusterAssignedJobBackend
	scheduler        *clusterScheduler
	logger           *slog.Logger
	appID            int64
	repository       string
	scaleSet         string
	installationID   githubapp.InstallationID
	githubScaleSetID int
	controllerEpoch  int64
	generation       uint64
	statisticsMutex  sync.RWMutex
	statistics       *upstream.RunnerScaleSetStatistic
}

func (recovery *clusterAssignedJobRecovery) start(ctx context.Context) {
	if recovery.latest() != nil && recovery.scheduler != nil {
		recovery.scheduler.wake(ctx, recovery.scaleSet)
	}
}

func (recovery *clusterAssignedJobRecovery) observe(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
	trigger string,
) error {
	if statistics == nil {
		return errors.New("scale set session statistics are required")
	}
	recovery.remember(statistics)
	logScaleSetSessionStatistics(ctx, recovery.logger, recovery.repository, recovery.scaleSet, trigger, statistics)
	if recovery.backend == nil {
		return errors.New("cluster assigned-job observation backend is required")
	}
	observationCtx, cancelObservation := context.WithTimeout(ctx, clusterCoordinationTimeout)
	capacity, err := recovery.backend.Prepare(observationCtx, statistics.TotalAssignedJobs)
	cancelObservation()
	if err != nil {
		return fmt.Errorf("persist clustered capacity obligations: %w", err)
	}
	recovery.logStatistics(ctx, statistics, trigger, "completed", nil,
		slog.Int("runningRunners", capacity.ActiveRunners), slog.Int("queuedRunners", statistics.TotalAssignedJobs-capacity.ActiveRunners))
	logCapacityObligationState(ctx, recovery.logger, recovery.generation, scaler.MessageUpdate{
		Repository: recovery.repository, ScaleSet: recovery.scaleSet,
		ControllerEpoch: recovery.controllerEpoch, ObservedAt: recovery.backend.latestObservedAt,
	}, recovery.backend.latestResult.QueueUpdate)
	if recovery.scheduler != nil {
		recovery.scheduler.wake(ctx, recovery.scaleSet)
	}
	return nil
}

func (recovery *clusterAssignedJobRecovery) prepareInitial(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
) error {
	return recovery.observe(ctx, statistics, "session")
}

func newClusterAssignedJobRecovery(
	leader *clusterLeader,
	session *repositoryScaleSetSession,
) *clusterAssignedJobRecovery {
	desired, _ := leader.currentDesiredRepository(session.key.repository)
	backend := &clusterAssignedJobBackend{
		state: leader.daemon.assignments, appID: leader.repositoryAppID,
		repository: session.key.repository, scaleSet: session.key.scaleSet,
		controllerEpoch: leader.repositoryControllerEpoch, now: leader.now,
	}
	return &clusterAssignedJobRecovery{
		logger: leader.daemon.logger, backend: backend, scheduler: leader.scheduler,
		appID:      leader.repositoryAppID,
		repository: session.key.repository, scaleSet: session.key.scaleSet,
		installationID: session.installationID, githubScaleSetID: session.runnerScaleSet.ID,
		controllerEpoch: leader.repositoryControllerEpoch, generation: desired.generation,
	}
}

func (recovery *clusterAssignedJobRecovery) remember(statistics *upstream.RunnerScaleSetStatistic) {
	copyOfStatistics := *statistics
	recovery.statisticsMutex.Lock()
	recovery.statistics = &copyOfStatistics
	recovery.statisticsMutex.Unlock()
}

func (recovery *clusterAssignedJobRecovery) latest() *upstream.RunnerScaleSetStatistic {
	recovery.statisticsMutex.RLock()
	defer recovery.statisticsMutex.RUnlock()
	if recovery.statistics == nil {
		return nil
	}
	copyOfStatistics := *recovery.statistics
	return &copyOfStatistics
}

func (recovery *clusterAssignedJobRecovery) logStatistics(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
	trigger string,
	phase string,
	err error,
	extra ...slog.Attr,
) {
	attributes := make([]slog.Attr, 0, 13+len(extra))
	attributes = append(attributes,
		slog.String("repository", recovery.repository), slog.String("scaleSet", recovery.scaleSet),
		slog.Int("assignedJobs", statistics.TotalAssignedJobs), slog.Int("availableJobs", statistics.TotalAvailableJobs),
		slog.Int("acquiredJobs", statistics.TotalAcquiredJobs), slog.Int("runningJobs", statistics.TotalRunningJobs),
		slog.Int("registeredRunners", statistics.TotalRegisteredRunners), slog.Int("busyRunners", statistics.TotalBusyRunners),
		slog.Int("idleRunners", statistics.TotalIdleRunners), slog.Int64("epoch", recovery.controllerEpoch),
		slog.Int64("installationID", int64(recovery.installationID)), slog.Int("githubScaleSetID", recovery.githubScaleSetID),
		slog.String("trigger", trigger),
	)
	attributes = append(attributes, extra...)
	if err != nil {
		attributes = append(attributes, slog.Duration("retryAfter", assignedJobRecoveryInterval))
		logActionFailure(ctx, recovery.logger, "assigned_recovery", err, attributes...)
		return
	}
	logAction(ctx, recovery.logger, slog.LevelInfo, "assigned_recovery", phase, attributes...)
}
