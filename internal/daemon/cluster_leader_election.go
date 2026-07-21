package daemon

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
)

func (daemon *clusteredDaemon) tryLeadership(ctx context.Context) {
	result, err := daemon.tryLeader(ctx)
	if errors.Is(err, cluster.ErrNotLeader) {
		return
	}
	if err != nil {
		daemon.logger.Error("leader election failed",
			slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		return
	}
	if result.lease == nil {
		return
	}
	daemon.acceptLeadership(ctx, result.lease)
}

func (daemon *clusteredDaemon) acceptLeadership(ctx context.Context, lease cluster.Lease) {
	daemon.leadershipMutex.Lock()
	daemon.leader = newClusterLeader(daemon, lease)
	daemon.leadershipMutex.Unlock()
	logAction(ctx, daemon.logger, slog.LevelInfo, "leader_election", "acquired",
		slog.String("hostname", string(daemon.nodeID)))
}

func (daemon *clusteredDaemon) currentLeader() *clusterLeader {
	daemon.leadershipMutex.Lock()
	defer daemon.leadershipMutex.Unlock()
	return daemon.leader
}

func (daemon *clusteredDaemon) clearLeadership(expected *clusterLeader) bool {
	daemon.leadershipMutex.Lock()
	defer daemon.leadershipMutex.Unlock()
	if daemon.leader != expected {
		return false
	}
	daemon.leader = nil
	return true
}

type clusterStatusNotifier interface {
	Status(ctx context.Context, status clusterServiceStatus) error
}

func (daemon *clusteredDaemon) notifyStatus(ctx context.Context, status clusterServiceStatus) {
	notifier, supported := daemon.notifier.(clusterStatusNotifier)
	if !supported {
		return
	}
	if err := notifier.Status(ctx, status); err != nil {
		daemon.logger.Warn("notify clustered service status failed",
			slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
	}
}

func (daemon *clusteredDaemon) startContender(ctx context.Context) {
	daemon.leadershipMutex.Lock()
	if daemon.contenderDone != nil {
		daemon.leadershipMutex.Unlock()
		return
	}
	contenderCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	daemon.stopContender = cancel
	daemon.contenderDone = done
	results := daemon.leadershipResults
	if results == nil {
		results = make(chan leadershipResult, 1)
		daemon.leadershipResults = results
	}
	daemon.leadershipMutex.Unlock()
	go daemon.contendForLeadership(contenderCtx, results, done)
}

func (daemon *clusteredDaemon) clearContenderRegistration() {
	daemon.leadershipMutex.Lock()
	daemon.stopContender = nil
	daemon.contenderDone = nil
	daemon.leadershipMutex.Unlock()
}

func (daemon *clusteredDaemon) contendForLeadership(
	ctx context.Context,
	results chan<- leadershipResult,
	done chan<- struct{},
) {
	defer close(done)
	for {
		result, err := daemon.waitForLeader(ctx)
		if err == nil {
			daemon.deliverLeadership(ctx, results, result.lease)
			return
		}
		if ctx.Err() != nil {
			return
		}
		daemon.logger.Error("leader contender failed",
			slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		if !waitForLeadershipRetry(ctx, daemon.leadershipRetryDelay) {
			return
		}
	}
}

func (daemon *clusteredDaemon) deliverLeadership(
	ctx context.Context,
	results chan<- leadershipResult,
	lease cluster.Lease,
) {
	select {
	case results <- leadershipResult{lease: lease}:
	case <-ctx.Done():
		if err := lease.Close(context.WithoutCancel(ctx)); err != nil {
			daemon.logger.Warn("close canceled leader lease failed",
				slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		}
	}
}

func waitForLeadershipRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = clusterLeadershipRetryDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (daemon *clusteredDaemon) stopLeadershipContender(ctx context.Context) {
	daemon.leadershipMutex.Lock()
	stopContender := daemon.stopContender
	contenderDone := daemon.contenderDone
	results := daemon.leadershipResults
	daemon.stopContender = nil
	daemon.contenderDone = nil
	daemon.leadershipMutex.Unlock()
	if stopContender == nil {
		return
	}
	stopContender()
	<-contenderDone
	select {
	case result := <-results:
		if err := result.lease.Close(context.WithoutCancel(ctx)); err != nil {
			daemon.logger.Warn("close unclaimed leader lease failed",
				slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		}
	default:
	}
}
