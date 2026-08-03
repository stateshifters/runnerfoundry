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
	leader.desiredMutex.Lock()
	if leader.desiredStateKnown && !sameScaleSetStatuses(leader.desiredRepositories[repository], statuses) {
		leader.desiredMutex.Unlock()
		return errRepositoryWorkObsolete
	}
	leader.desiredMutex.Unlock()
	leader.sessionsMutex.Lock()
	for _, session := range started {
		leader.sessions[session.key] = session
	}
	leader.sessionsMutex.Unlock()
	if err := leader.bootstrapRepositorySessionGeneration(ctx, started); err != nil {
		leader.detachRepositorySessionGeneration(started)
		return err
	}
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
	runtimeCtx, stopRuntime := context.WithCancel(context.WithoutCancel(ctx))
	session.stopPolling = stopRuntime
	session.assignedJobs.start(runtimeCtx)
	session.pollerDone = leader.startSessionPoller(runtimeCtx, session)
}

func (leader *clusterLeader) wakeAssignedJobRecovery(ctx context.Context) {
	if leader.assignedJobWorkers != nil {
		leader.assignedJobWorkers.wakeAssigned(ctx)
	}
}

type clusterAssignedJobBackend struct {
	state           clusterAssignedJobState
	appID           int64
	repository      string
	scaleSet        string
	controllerEpoch int64
	now             func() time.Time
	assignedJobs    int
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
	capacity, err := backend.state.ObserveAssignedJobs(ctx, backend.observation(assignedJobs))
	if err != nil {
		return scaler.AssignedJobCapacity{}, fmt.Errorf("observe clustered assigned jobs: %w", err)
	}
	return capacity, nil
}

func (backend *clusterAssignedJobBackend) StartAssigned(ctx context.Context) (bool, error) {
	_, err := backend.state.AssignAssignedJob(ctx, backend.observation(backend.assignedJobs), backend.now().UTC(), clusterNodeLifetime)
	if errors.Is(err, cluster.ErrNoAssignment) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("assign clustered assigned job: %w", err)
	}
	return true, nil
}

type clusterAssignedJobRecovery struct {
	controller       *scaler.AssignedJobController
	logger           *slog.Logger
	repository       string
	scaleSet         string
	installationID   githubapp.InstallationID
	githubScaleSetID int
	controllerEpoch  int64
	statisticsMutex  sync.RWMutex
	statistics       *upstream.RunnerScaleSetStatistic
	runtimeDone      <-chan struct{}
	executor         *assignedJobRecoveryExecutor
}

func (recovery *clusterAssignedJobRecovery) start(ctx context.Context) {
	recovery.statisticsMutex.Lock()
	recovery.runtimeDone = ctx.Done()
	recovery.statisticsMutex.Unlock()
	if recovery.executor == nil {
		return
	}
	recovery.executor.register(recovery)
	if recovery.latest() != nil {
		recovery.executor.enqueue(ctx, recovery, "session")
	}
}

func (recovery *clusterAssignedJobRecovery) cancelAttemptWhenStopped(cancel context.CancelFunc) func() bool {
	recovery.statisticsMutex.RLock()
	runtimeDone := recovery.runtimeDone
	recovery.statisticsMutex.RUnlock()
	if runtimeDone == nil {
		return func() bool { return false }
	}
	finished := make(chan struct{})
	go func() {
		select {
		case <-runtimeDone:
			cancel()
		case <-finished:
		}
	}()
	return func() bool {
		close(finished)
		return true
	}
}

func (recovery *clusterAssignedJobRecovery) stop() {
	if recovery.executor != nil {
		recovery.executor.unregister(recovery)
	}
}

func (recovery *clusterAssignedJobRecovery) reconcileLatest(ctx context.Context, trigger string) error {
	statistics := recovery.latest()
	if statistics == nil {
		return nil
	}
	return recovery.reconcile(ctx, statistics, trigger)
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
	if recovery.executor != nil {
		recovery.executor.enqueue(ctx, recovery, trigger)
	}
	return nil
}

func (recovery *clusterAssignedJobRecovery) prepareInitial(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
) error {
	return recovery.observe(ctx, statistics, "session")
}

func (recovery *clusterAssignedJobRecovery) key() repositoryScaleSetSessionKey {
	return repositoryScaleSetSessionKey{repository: recovery.repository, scaleSet: recovery.scaleSet}
}

func (recovery *clusterAssignedJobRecovery) hasAssignedJobs() bool {
	statistics := recovery.latest()
	return statistics != nil && statistics.TotalAssignedJobs > 0
}

func newClusterAssignedJobRecovery(
	leader *clusterLeader,
	session *repositoryScaleSetSession,
) *clusterAssignedJobRecovery {
	backend := &clusterAssignedJobBackend{
		state: leader.daemon.assignments, appID: leader.repositoryAppID,
		repository: session.key.repository, scaleSet: session.key.scaleSet,
		controllerEpoch: leader.repositoryControllerEpoch, now: leader.now,
	}
	return &clusterAssignedJobRecovery{
		controller: scaler.NewAssignedJobController(backend), logger: leader.daemon.logger,
		repository: session.key.repository, scaleSet: session.key.scaleSet,
		installationID: session.installationID, githubScaleSetID: session.runnerScaleSet.ID,
		controllerEpoch: leader.repositoryControllerEpoch, executor: leader.assignedJobWorkers,
	}
}

func (recovery *clusterAssignedJobRecovery) reconcile(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
	trigger string,
) error {
	if statistics == nil {
		return errors.New("initial scale set session statistics are required")
	}
	recovery.logStatistics(ctx, statistics, trigger, "started", nil)
	result, err := recovery.controller.Reconcile(ctx, statistics.TotalAssignedJobs)
	if err != nil {
		recovery.logStatistics(ctx, statistics, trigger, "failed", err)
		return fmt.Errorf("reconcile clustered assigned jobs: %w", err)
	}
	recovery.logStatistics(ctx, statistics, trigger, "completed", nil,
		slog.Int("runningRunners", result.RunningRunners), slog.Int("startedRunners", result.StartedRunners))
	return nil
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
