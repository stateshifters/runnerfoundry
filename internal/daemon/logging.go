package daemon

import (
	"context"
	"log/slog"
)

func logAction(ctx context.Context, logger *slog.Logger, level slog.Level, action string, phase string, attributes ...slog.Attr) {
	if logger == nil {
		return
	}
	baseAttributes := []slog.Attr{
		slog.String("action", action),
		slog.String("phase", phase),
	}
	logger.LogAttrs(ctx, level, action, append(baseAttributes, attributes...)...)
}

func logActionFailure(ctx context.Context, logger *slog.Logger, action string, err error, attributes ...slog.Attr) {
	logAction(ctx, logger, slog.LevelError, action, "failed", append(attributes, slog.String("error", err.Error()))...)
}
