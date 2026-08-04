package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
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
	replacement := newClusterLeader(ctx, daemon, lease)
	daemon.leadershipMutex.Lock()
	previous := daemon.leader
	if previous != nil {
		previous.stopTerm()
	}
	daemon.leader = replacement
	daemon.leadershipMutex.Unlock()
	if previous != nil {
		previous.stop(context.WithoutCancel(ctx))
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "leader_election", "acquired",
		slog.String("hostname", string(daemon.nodeID)))
}

func (daemon *clusteredDaemon) closeLeadershipLease(ctx context.Context, lease cluster.Lease) error {
	closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), daemon.leadershipReleaseDuration())
	defer cancelClose()
	if err := lease.Close(closeCtx); err != nil {
		return fmt.Errorf("close leadership lease: %w", err)
	}
	return nil
}

func (daemon *clusteredDaemon) leadershipReleaseDuration() time.Duration {
	if daemon.leadershipReleaseTimeout > 0 {
		return daemon.leadershipReleaseTimeout
	}
	return clusterCoordinationTimeout
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

func (daemon *clusteredDaemon) startContender(ctx context.Context) bool {
	daemon.leadershipMutex.Lock()
	if daemon.contenderDone != nil {
		daemon.leadershipMutex.Unlock()
		return false
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
	return true
}

func (daemon *clusteredDaemon) enterStandby(ctx context.Context) {
	if daemon.startContender(ctx) {
		logAction(ctx, daemon.logger, slog.LevelInfo, "leader_election", "contending",
			slog.String("hostname", string(daemon.nodeID)))
	}
	daemon.notifyStatus(ctx, clusterServiceStatusStandby)
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
		waitCtx, cancelWait := context.WithTimeout(ctx, daemon.leadershipWaitDuration())
		result, err := daemon.waitForLeader(waitCtx)
		waitExpired := errors.Is(waitCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		cancelWait()
		if err == nil {
			daemon.deliverLeadership(ctx, results, result.lease)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if waitExpired {
			continue
		}
		daemon.logger.Error("leader contender failed",
			slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		if !daemon.waitForLeadershipRetry(ctx) {
			return
		}
	}
}

func (daemon *clusteredDaemon) leadershipWaitDuration() time.Duration {
	if daemon.leadershipWaitTimeout > 0 {
		return daemon.leadershipWaitTimeout
	}
	return clusterLeadershipWaitTimeout
}

func (daemon *clusteredDaemon) deliverLeadership(
	ctx context.Context,
	results chan<- leadershipResult,
	lease cluster.Lease,
) {
	select {
	case results <- leadershipResult{lease: lease}:
	case <-ctx.Done():
		if err := daemon.closeLeadershipLease(ctx, lease); err != nil {
			daemon.logger.Warn("close canceled leader lease failed",
				slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		}
	}
}

func (daemon *clusteredDaemon) waitForLeadershipRetry(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	delay := daemon.leadershipRetryDelay
	if delay <= 0 {
		delay = clusterLeadershipRetryDelay
	}
	return daemon.waitBeforeLeadershipRetry(ctx, delay)
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
		if err := daemon.closeLeadershipLease(ctx, result.lease); err != nil {
			daemon.logger.Warn("close unclaimed leader lease failed",
				slog.String("hostname", string(daemon.nodeID)), slog.String("error", err.Error()))
		}
	default:
	}
}
