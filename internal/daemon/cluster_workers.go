package daemon

import (
	"context"
	"log/slog"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
)

func (daemon *clusteredDaemon) startClusterWorkers(ctx context.Context, events clusterEvents) chan<- struct{} {
	reconciliationRequests := make(chan struct{}, 1)
	daemon.workers.Add(3)
	go daemon.runHeartbeatLoop(ctx, events.heartbeat)
	go daemon.runExecutorLoop(ctx, events.executor)
	go daemon.runReconciliationLoop(ctx, reconciliationRequests)
	return reconciliationRequests
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
	}
	leader := daemon.currentLeader()
	leaseCtx, cancelLeaseCheck := context.WithTimeout(ctx, clusterCoordinationTimeout)
	leaseAlive := leader == nil || leader.lease.Alive(leaseCtx)
	cancelLeaseCheck()
	if leader != nil && !leaseAlive {
		if daemon.clearLeadership(leader) {
			leader.stop(context.WithoutCancel(ctx))
		}
		leader = nil
	}
	if leader == nil {
		daemon.notifyStatus(ctx, clusterServiceStatusStandby)
		daemon.startContender(ctx)
	}
}
