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
		if err := session.assignedJobs.reconcile(ctx, session.messageClient.initialStatistics(), "session"); err != nil {
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
	session.assignedJobs.signal()
	session.pollerDone = leader.startSessionPoller(runtimeCtx, session)
}

func (leader *clusterLeader) wakeAssignedJobRecovery() {
	leader.sessionsMutex.Lock()
	sessions := make([]*repositoryScaleSetSession, 0, len(leader.sessions))
	for _, session := range leader.sessions {
		sessions = append(sessions, session)
	}
	leader.sessionsMutex.Unlock()
	for _, session := range sessions {
		if session.assignedJobs != nil {
			session.assignedJobs.signal()
		}
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
	wake             chan struct{}
	done             <-chan struct{}
}

func (recovery *clusterAssignedJobRecovery) start(ctx context.Context) {
	done := make(chan struct{})
	recovery.done = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(assignedJobRecoveryInterval)
		defer ticker.Stop()
		recovery.run(ctx, ticker.C)
	}()
}

func (recovery *clusterAssignedJobRecovery) run(ctx context.Context, safetyTicks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-recovery.wake:
			recovery.reconcileLatest(ctx, "wake")
		case <-safetyTicks:
			recovery.reconcileLatest(ctx, "safety")
		}
	}
}

func (recovery *clusterAssignedJobRecovery) reconcileLatest(ctx context.Context, source string) {
	statistics := recovery.latest()
	if statistics == nil {
		return
	}
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, clusterCoordinationTimeout)
	if err := recovery.reconcile(recoveryCtx, statistics, source); err != nil {
		cancelRecovery()
		return
	}
	cancelRecovery()
}

func (recovery *clusterAssignedJobRecovery) signal() {
	select {
	case recovery.wake <- struct{}{}:
	default:
	}
}

func (recovery *clusterAssignedJobRecovery) wait() {
	if recovery.done != nil {
		<-recovery.done
	}
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
		controllerEpoch: leader.repositoryControllerEpoch, wake: make(chan struct{}, 1),
	}
}

func (recovery *clusterAssignedJobRecovery) reconcile(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
	source string,
) error {
	if statistics == nil {
		return errors.New("initial scale set session statistics are required")
	}
	recovery.remember(statistics)
	if source == "session" || source == "message" {
		logScaleSetSessionStatistics(ctx, recovery.logger, recovery.repository, recovery.scaleSet, source, statistics)
	}
	recovery.logStatistics(ctx, statistics, source, "started", nil)
	result, err := recovery.controller.Reconcile(ctx, statistics.TotalAssignedJobs)
	if err != nil {
		recovery.logStatistics(ctx, statistics, source, "failed", err)
		return fmt.Errorf("reconcile clustered assigned jobs: %w", err)
	}
	recovery.logStatistics(ctx, statistics, source, "completed", nil,
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
	source string,
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
		slog.String("source", source),
	)
	attributes = append(attributes, extra...)
	if err != nil {
		attributes = append(attributes, slog.Duration("retryAfter", assignedJobRecoveryInterval))
		logActionFailure(ctx, recovery.logger, "assigned_recovery", err, attributes...)
		return
	}
	logAction(ctx, recovery.logger, slog.LevelInfo, "assigned_recovery", phase, attributes...)
}
