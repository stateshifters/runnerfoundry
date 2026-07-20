package daemon

import (
	"context"
	"log/slog"
	"time"

	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

func pollUntilShutdown(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	poller *scalesetpoller.Poller,
	retryDelay time.Duration,
) {
	consecutiveFailures := 0
	for ctx.Err() == nil {
		pollStartedAt := time.Now()
		logMessagePoll(ctx, logger, repositoryURL, scaleSetName, "started")
		if err := poller.PollOnce(ctx); err != nil {
			if ctx.Err() != nil {
				logMessagePollFinished(ctx, logger, repositoryURL, scaleSetName, "canceled", pollStartedAt, poller.LastMessageID)
				return
			}
			consecutiveFailures = logPollFailure(logger, repositoryURL, scaleSetName, err, consecutiveFailures)
			if !waitForPollRetry(ctx, retryDelay) {
				return
			}
			continue
		}
		consecutiveFailures = 0
		logMessagePollFinished(ctx, logger, repositoryURL, scaleSetName, "completed", pollStartedAt, poller.LastMessageID)
	}
}

func logMessagePoll(ctx context.Context, logger *slog.Logger, repositoryURL string, scaleSetName string, phase string) {
	logAction(ctx, logger, slog.LevelDebug, "message_poll", phase,
		slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName))
}

func logMessagePollFinished(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	phase string,
	startedAt time.Time,
	lastMessageID int,
) {
	logAction(context.WithoutCancel(ctx), logger, slog.LevelDebug, "message_poll", phase,
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
		slog.Int("lastMessageID", lastMessageID),
		slog.Duration("duration", time.Since(startedAt)),
	)
}

func logPollFailure(logger *slog.Logger, repositoryURL string, scaleSetName string, pollErr error, consecutiveFailures int) int {
	consecutiveFailures++
	logger.Error("poll failed",
		slog.String("action", "message_poll"),
		slog.String("phase", "failed"),
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
		slog.String("error", pollErr.Error()),
	)
	if consecutiveFailures == sustainedPollFailureThreshold {
		logger.Error(
			"sustained poll failures",
			slog.String("action", "message_poll"),
			slog.String("phase", "degraded"),
			slog.String("repository", repositoryURL),
			slog.String("scaleSet", scaleSetName),
			slog.Int("consecutiveFailures", consecutiveFailures),
		)
	}
	return consecutiveFailures
}

func waitForPollRetry(ctx context.Context, retryDelay time.Duration) bool {
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
