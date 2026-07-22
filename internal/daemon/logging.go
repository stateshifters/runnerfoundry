package daemon

import (
	"context"
	"log/slog"
	"time"
)

func logAction(ctx context.Context, logger *slog.Logger, level slog.Level, action string, phase string, attributes ...slog.Attr) {
	if logger == nil {
		return
	}
	baseAttributes := make([]slog.Attr, 0, 2+len(attributes))
	baseAttributes = append(baseAttributes, slog.String("action", action), slog.String("phase", phase))
	logger.LogAttrs(ctx, level, action, append(baseAttributes, attributes...)...)
}

func logActionFailure(ctx context.Context, logger *slog.Logger, action string, err error, attributes ...slog.Attr) {
	logAction(ctx, logger, slog.LevelError, action, "failed", append(attributes, slog.String("error", err.Error()))...)
}

type tracedOperationResult struct {
	terminalAttributes []slog.Attr
	err                error
}

func runTracedOperation(
	ctx context.Context,
	logger *slog.Logger,
	successLevel slog.Level,
	action string,
	initialAttributes []slog.Attr,
	operation func() tracedOperationResult,
) error {
	startedAt := time.Now()
	logAction(ctx, logger, successLevel, action, "started", initialAttributes...)
	result := operation()
	terminalAttributes := make([]slog.Attr, 0, len(initialAttributes)+len(result.terminalAttributes)+1)
	terminalAttributes = append(terminalAttributes, initialAttributes...)
	terminalAttributes = append(terminalAttributes, result.terminalAttributes...)
	terminalAttributes = append(terminalAttributes, slog.Duration("duration", time.Since(startedAt)))
	if result.err != nil {
		logActionFailure(ctx, logger, action, result.err, terminalAttributes...)
		return result.err
	}
	logAction(ctx, logger, successLevel, action, "completed", terminalAttributes...)
	return nil
}
