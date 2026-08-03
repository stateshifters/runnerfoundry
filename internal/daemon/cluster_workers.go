package daemon

import (
	"context"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
)

func (daemon *clusteredDaemon) startClusterWorkers(ctx context.Context, events clusterEvents) chan<- struct{} {
	reconciliationRequests := make(chan struct{}, 1)
	daemon.repositoryRepairRequests = make(chan struct{}, 1)
	daemon.workers.Add(5)
	go daemon.runHeartbeatLoop(ctx, events.heartbeat)
	go daemon.runExecutorLoop(ctx, events.executor)
	go daemon.runReconciliationLoop(ctx, reconciliationRequests)
	go daemon.runRepositoryRepairLoop(ctx)
	go daemon.runPeriodicReconciliationLoop(ctx, reconciliationRequests)
	return reconciliationRequests
}

func (daemon *clusteredDaemon) runPeriodicReconciliationLoop(ctx context.Context, requests chan<- struct{}) {
	defer daemon.workers.Done()
	for {
		delay, err := daemon.configurationSnapshot().config.GitHub.DiscoveryDuration()
		if err != nil {
			daemon.logger.Error("load clustered registration reconciliation interval failed", slog.String("error", err.Error()))
			delay = pollRetryDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			requestReconciliation(requests)
		}
	}
}

func (daemon *clusteredDaemon) runHeartbeatLoop(ctx context.Context, ticks <-chan time.Time) {
	defer daemon.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			daemon.heartbeat(ctx)
		}
	}
}

func (daemon *clusteredDaemon) runExecutorLoop(ctx context.Context, ticks <-chan time.Time) {
	defer daemon.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			daemon.executeAssignments(ctx)
		}
	}
}

func (daemon *clusteredDaemon) runReconciliationLoop(ctx context.Context, requests <-chan struct{}) {
	defer daemon.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
			daemon.reconcileLeader(ctx)
		}
	}
}

func (daemon *clusteredDaemon) runRepositoryRepairLoop(ctx context.Context) {
	defer daemon.workers.Done()
	repairTimer := time.NewTimer(time.Hour)
	repairTimer.Stop()
	defer repairTimer.Stop()
	for {
		repairReady := daemon.resetRepositoryRepairTimer(repairTimer)
		select {
		case <-ctx.Done():
			return
		case <-daemon.repositoryRepairRequests:
			continue
		case <-repairReady:
			daemon.repairLeaderRepositories(ctx)
		}
	}
}

func (daemon *clusteredDaemon) resetRepositoryRepairTimer(timer *time.Timer) <-chan time.Time {
	timer.Stop()
	leader := daemon.currentLeader()
	if leader == nil {
		return nil
	}
	delay, pending := leader.nextRepositoryRepairDelay()
	if !pending {
		return nil
	}
	timer.Reset(delay)
	return timer.C
}

func (daemon *clusteredDaemon) repairLeaderRepositories(ctx context.Context) {
	leader := daemon.currentLeader()
	if leader != nil {
		leader.repairPendingRepositories(ctx)
	}
}

func (daemon *clusteredDaemon) reconcileLeader(ctx context.Context) {
	leader := daemon.currentLeader()
	if leader == nil {
		return
	}
	daemon.notifyStatus(ctx, clusterServiceStatusLeaderReconciling)
	if leader.reconcile(ctx) {
		daemon.notifyStatus(ctx, clusterServiceStatusLeaderOperational)
		return
	}
	daemon.notifyStatus(ctx, clusterServiceStatusLeaderDegraded)
}

func requestReconciliation(requests chan<- struct{}) {
	select {
	case requests <- struct{}{}:
	default:
	}
}

func (daemon *clusteredDaemon) waitForWorkers() {
	daemon.workers.Wait()
}

func (daemon *clusteredDaemon) heartbeat(ctx context.Context) {
	if err := daemon.publish(ctx, cluster.NodeHealthy); err != nil {
		daemon.logger.Error("cluster heartbeat failed",
			slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
	} else if leader := daemon.currentLeader(); leader != nil {
		leader.wakeAssignedJobRecovery()
	}
	leader := daemon.currentLeader()
	leaseCtx, cancelLeaseCheck := context.WithTimeout(ctx, clusterCoordinationTimeout)
	leaseAlive := leader == nil || leader.lease.Alive(leaseCtx)
	cancelLeaseCheck()
	if leader != nil && !leaseAlive {
		if daemon.clearLeadership(leader) {
			logAction(context.WithoutCancel(ctx), daemon.logger, slog.LevelWarn, "leader_election", "lost",
				slog.String("hostname", string(daemon.nodeID)))
			leader.stop(context.WithoutCancel(ctx))
		}
		leader = nil
	}
	if leader == nil {
		daemon.notifyStatus(ctx, clusterServiceStatusStandby)
		daemon.startContender(ctx)
	}
}
