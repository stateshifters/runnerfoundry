package daemon

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

const sessionCloseWorkerCount = 4

func (leader *clusterLeader) stopLeaderSessions(
	ctx context.Context,
	sessions map[repositoryScaleSetSessionKey]*repositoryScaleSetSession,
) {
	if len(sessions) == 0 {
		return
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, leader.githubOperationTimeout)
	defer cancelShutdown()
	jobs := make(chan *repositoryScaleSetSession, len(sessions))
	for _, session := range sessions {
		jobs <- session
	}
	close(jobs)
	var remaining atomic.Int64
	remaining.Store(int64(len(sessions)))
	var workers sync.WaitGroup
	workers.Add(sessionCloseWorkerCount)
	for workerIndex := 0; workerIndex < sessionCloseWorkerCount; workerIndex++ {
		go leader.runSessionCloseWorker(shutdownCtx, jobs, &remaining, &workers)
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		logAction(context.WithoutCancel(ctx), leader.daemon.logger, slog.LevelWarn,
			"leader_shutdown", "session_close_timeout",
			slog.Int64("remainingSessions", remaining.Load()))
	}
}

func (leader *clusterLeader) runSessionCloseWorker(
	ctx context.Context,
	jobs <-chan *repositoryScaleSetSession,
	remaining *atomic.Int64,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case session, available := <-jobs:
			if !available {
				return
			}
			err := leader.stopSession(ctx, session, retainGitHubScaleSet)
			remaining.Add(-1)
			if err != nil && ctx.Err() == nil {
				leader.daemon.logger.Error("stop leader session failed",
					slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
			}
		}
	}
}
