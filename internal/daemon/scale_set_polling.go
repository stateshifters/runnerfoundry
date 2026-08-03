package daemon

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

const pollRetryDelay = 5 * time.Second
const sustainedPollFailureThreshold = 3

type pollRetryPolicy struct {
	delay time.Duration
	wait  func(context.Context, time.Duration) bool
}

type failedPoll struct {
	logger              *slog.Logger
	repositoryURL       string
	scaleSetName        string
	startedAt           time.Time
	lastMessageID       int
	err                 error
	consecutiveFailures int
}

func pollRetryAfter(delay time.Duration) pollRetryPolicy {
	return pollRetryPolicy{delay: delay, wait: waitForRetryDelay}
}

func (retry pollRetryPolicy) waitBeforeNext(ctx context.Context) bool {
	return retry.wait(ctx, retry.delay)
}

func pollUntilShutdown(
	ctx context.Context,
	logger *slog.Logger,
	repositoryURL string,
	scaleSetName string,
	poller *scalesetpoller.Poller,
	retry pollRetryPolicy,
) {
	consecutiveFailures := 0
	for ctx.Err() == nil {
		pollStartedAt := time.Now()
		logMessagePoll(ctx, logger, repositoryURL, scaleSetName, "started")
		pollErr := poller.PollOnce(ctx)
		if pollErr == nil {
			consecutiveFailures = 0
			logMessagePollFinished(ctx, logger, repositoryURL, scaleSetName, "completed", pollStartedAt, poller.LastMessageID)
			continue
		}
		var continuePolling bool
		consecutiveFailures, continuePolling = handlePollFailure(
			ctx,
			failedPoll{
				logger: logger, repositoryURL: repositoryURL, scaleSetName: scaleSetName,
				startedAt: pollStartedAt, lastMessageID: poller.LastMessageID,
				err: pollErr, consecutiveFailures: consecutiveFailures,
			},
			retry,
		)
		if !continuePolling {
			return
		}
	}
}

func handlePollFailure(
	ctx context.Context,
	failure failedPoll,
	retry pollRetryPolicy,
) (int, bool) {
	if ctx.Err() != nil {
		logMessagePollFinished(ctx, failure.logger, failure.repositoryURL, failure.scaleSetName,
			"canceled", failure.startedAt, failure.lastMessageID)
		return failure.consecutiveFailures, false
	}
	consecutiveFailures := logPollFailure(
		failure.logger, failure.repositoryURL, failure.scaleSetName, failure.err, failure.consecutiveFailures,
	)
	if errors.Is(failure.err, scaler.ErrNotLeader) {
		return consecutiveFailures, false
	}
	return consecutiveFailures, retry.waitBeforeNext(ctx)
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
