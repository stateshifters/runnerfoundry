package daemon

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

const sessionCloseWorkerCount = 4

type sessionShutdownProgress struct {
	totalSessions   int
	canceledPollers int
	remaining       atomic.Int64
	remotelyClosed  atomic.Int64
	failures        atomic.Int64
}

func newSessionShutdownProgress(totalSessions int) *sessionShutdownProgress {
	progress := &sessionShutdownProgress{totalSessions: totalSessions, canceledPollers: totalSessions}
	progress.remaining.Store(int64(totalSessions))
	return progress
}

func (progress *sessionShutdownProgress) recordRemoteClose(err error) {
	progress.remaining.Add(-1)
	if err == nil {
		progress.remotelyClosed.Add(1)
		return
	}
	progress.failures.Add(1)
}

func (progress *sessionShutdownProgress) remainingCleanup() int64 {
	return int64(progress.totalSessions) - progress.remotelyClosed.Load()
}

func (leader *clusterLeader) stopLeaderSessions(
	ctx context.Context,
	sessions map[repositoryScaleSetSessionKey]*repositoryScaleSetSession,
) {
	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, leader.githubOperationTimeout)
	defer cancelShutdown()
	progress := newSessionShutdownProgress(len(sessions))
	jobs := stopSessionPollers(sessions)
	done := leader.startSessionCloseWorkers(shutdownCtx, jobs, progress)
	leader.waitForSessionCloseWorkers(ctx, shutdownCtx, done, progress)
	leader.logSessionCloseSummary(ctx, progress)
}

func stopSessionPollers(
	sessions map[repositoryScaleSetSessionKey]*repositoryScaleSetSession,
) <-chan *repositoryScaleSetSession {
	jobs := make(chan *repositoryScaleSetSession, len(sessions))
	for _, session := range sessions {
		session.stopPolling()
		jobs <- session
	}
	close(jobs)
	return jobs
}

func (leader *clusterLeader) startSessionCloseWorkers(
	ctx context.Context,
	jobs <-chan *repositoryScaleSetSession,
	progress *sessionShutdownProgress,
) <-chan struct{} {
	var workers sync.WaitGroup
	workers.Add(sessionCloseWorkerCount)
	for workerIndex := 0; workerIndex < sessionCloseWorkerCount; workerIndex++ {
		go leader.runSessionCloseWorker(ctx, jobs, progress, &workers)
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	return done
}

func (leader *clusterLeader) waitForSessionCloseWorkers(
	ctx context.Context,
	shutdownCtx context.Context,
	done <-chan struct{},
	progress *sessionShutdownProgress,
) {
	select {
	case <-done:
	case <-shutdownCtx.Done():
		logAction(context.WithoutCancel(ctx), leader.daemon.logger, slog.LevelWarn,
			"leader_shutdown", "session_close_timeout",
			slog.Int64("remainingSessions", progress.remaining.Load()))
	}
}

func (leader *clusterLeader) logSessionCloseSummary(ctx context.Context, progress *sessionShutdownProgress) {
	logAction(context.WithoutCancel(ctx), leader.daemon.logger, slog.LevelInfo,
		"leader_shutdown", "session_close_summary",
		slog.Int64("epoch", leader.repositoryControllerEpoch),
		slog.Int("totalSessions", progress.totalSessions),
		slog.Int("canceledPollers", progress.canceledPollers),
		slog.Int64("remotelyClosedSessions", progress.remotelyClosed.Load()),
		slog.Int64("failures", progress.failures.Load()),
		slog.Int64("remainingCleanup", progress.remainingCleanup()))
}

func (leader *clusterLeader) runSessionCloseWorker(
	ctx context.Context,
	jobs <-chan *repositoryScaleSetSession,
	progress *sessionShutdownProgress,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	for {
		session, available := nextSessionToClose(ctx, jobs)
		if !available {
			return
		}
		leader.closeSessionRemotely(ctx, session, progress)
	}
}

func nextSessionToClose(
	ctx context.Context,
	jobs <-chan *repositoryScaleSetSession,
) (*repositoryScaleSetSession, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case session, available := <-jobs:
		return session, available
	}
}

func (leader *clusterLeader) closeSessionRemotely(
	ctx context.Context,
	session *repositoryScaleSetSession,
	progress *sessionShutdownProgress,
) {
	err := leader.stopSession(ctx, session, retainGitHubScaleSet)
	progress.recordRemoteClose(err)
	if err != nil && ctx.Err() == nil {
		leader.daemon.logger.Error("stop leader session failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
	}
}
