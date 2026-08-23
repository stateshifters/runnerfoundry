package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/stateshifters/runnerfoundry/internal/config"
)

type logDestinationFactory interface {
	Open(logConfig config.LogConfig) (io.WriteCloser, error)
}

type loggingManager struct {
	factory logDestinationFactory
	handler *reloadableLogHandler
	logger  *slog.Logger
}

func newLoggingManager(logConfig config.LogConfig, factory logDestinationFactory) (*loggingManager, error) {
	target, err := openLoggingTarget(logConfig, factory)
	if err != nil {
		return nil, err
	}
	handler := newReloadableLogHandler(target)
	return &loggingManager{factory: factory, handler: handler, logger: slog.New(handler)}, nil
}

func (manager *loggingManager) Logger() *slog.Logger {
	return manager.logger
}

func (manager *loggingManager) Prepare(logConfig config.LogConfig) (*preparedLogging, error) {
	target, err := openLoggingTarget(logConfig, manager.factory)
	if err != nil {
		return nil, err
	}
	return &preparedLogging{manager: manager, target: target}, nil
}

func (manager *loggingManager) Close() error {
	return manager.handler.replace(loggingTarget{handler: slog.DiscardHandler, destination: noOpWriteCloser{writer: io.Discard}})
}

type preparedLogging struct {
	manager        *loggingManager
	target         loggingTarget
	previous       *loggingTarget
	cleanup        io.WriteCloser
	cleanupFailure string
	mutex          sync.Mutex
	completed      bool
	activated      bool
}

func (prepared *preparedLogging) Activate() error {
	prepared.mutex.Lock()
	defer prepared.mutex.Unlock()
	if prepared.completed || prepared.activated {
		return errors.New("prepared logging configuration is already completed")
	}
	previous := prepared.manager.handler.swap(prepared.target)
	prepared.previous = &previous
	prepared.activated = true
	return nil
}

func (prepared *preparedLogging) Commit() error {
	prepared.mutex.Lock()
	defer prepared.mutex.Unlock()
	if !prepared.activated || prepared.completed {
		return errors.New("prepared logging configuration is not active")
	}
	prepared.completed = true
	prepared.cleanup = prepared.previous.destination
	prepared.cleanupFailure = "close replaced log destination"
	prepared.attemptCleanupWithoutFailingCommit()
	return nil
}

func (prepared *preparedLogging) Rollback() error {
	prepared.mutex.Lock()
	defer prepared.mutex.Unlock()
	if !prepared.activated || prepared.completed {
		return nil
	}
	candidate := prepared.manager.handler.swap(*prepared.previous)
	prepared.completed = true
	prepared.cleanup = candidate.destination
	prepared.cleanupFailure = "close replacement log destination"
	return prepared.attemptCleanup()
}

func (prepared *preparedLogging) Close() error {
	prepared.mutex.Lock()
	defer prepared.mutex.Unlock()
	if !prepared.completed {
		prepared.completed = true
		if prepared.activated {
			candidate := prepared.manager.handler.swap(*prepared.previous)
			prepared.cleanup = candidate.destination
			prepared.cleanupFailure = "close rolled-back log destination"
		} else {
			prepared.cleanup = prepared.target.destination
			prepared.cleanupFailure = "close prepared log destination"
		}
	}
	return prepared.attemptCleanup()
}

func (prepared *preparedLogging) attemptCleanup() error {
	if prepared.cleanup == nil {
		return nil
	}
	if err := prepared.cleanup.Close(); err != nil {
		return fmt.Errorf("%s: %w", prepared.cleanupFailure, err)
	}
	prepared.cleanup = nil
	return nil
}

func (prepared *preparedLogging) attemptCleanupWithoutFailingCommit() {
	if prepared.cleanup != nil && prepared.cleanup.Close() == nil {
		prepared.cleanup = nil
	}
}

type loggingTarget struct {
	handler     slog.Handler
	destination io.WriteCloser
}

func openLoggingTarget(logConfig config.LogConfig, factory logDestinationFactory) (loggingTarget, error) {
	destination, err := factory.Open(logConfig)
	if err != nil {
		return loggingTarget{}, fmt.Errorf("open log destination: %w", err)
	}
	handlerOptions := &slog.HandlerOptions{Level: configuredLogLevel(logConfig.Level)}
	if logConfig.Format == "json" {
		handlerOptions.ReplaceAttr = renameJSONTimestamp
		return loggingTarget{handler: slog.NewJSONHandler(destination, handlerOptions), destination: destination}, nil
	}
	return loggingTarget{handler: slog.NewTextHandler(destination, handlerOptions), destination: destination}, nil
}

type reloadableLogHandler struct {
	state      *reloadableLogState
	decorators []logHandlerDecorator
}

type reloadableLogState struct {
	mutex  sync.RWMutex
	target loggingTarget
}

type logHandlerDecorator func(slog.Handler) slog.Handler

func newReloadableLogHandler(target loggingTarget) *reloadableLogHandler {
	return &reloadableLogHandler{state: &reloadableLogState{target: target}}
}

func (handler *reloadableLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	handler.state.mutex.RLock()
	defer handler.state.mutex.RUnlock()
	return handler.state.target.handler.Enabled(ctx, level)
}

func (handler *reloadableLogHandler) Handle(ctx context.Context, record slog.Record) error {
	handler.state.mutex.RLock()
	defer handler.state.mutex.RUnlock()
	activeHandler := handler.decorated(handler.state.target.handler)
	if !activeHandler.Enabled(ctx, record.Level) {
		return nil
	}
	if err := activeHandler.Handle(ctx, record); err != nil {
		return fmt.Errorf("handle log record: %w", err)
	}
	return nil
}

func (handler *reloadableLogHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	copiedAttributes := append([]slog.Attr(nil), attributes...)
	return handler.withDecorator(func(active slog.Handler) slog.Handler {
		return active.WithAttrs(copiedAttributes)
	})
}

func (handler *reloadableLogHandler) WithGroup(name string) slog.Handler {
	return handler.withDecorator(func(active slog.Handler) slog.Handler {
		return active.WithGroup(name)
	})
}

func (handler *reloadableLogHandler) withDecorator(decorator logHandlerDecorator) *reloadableLogHandler {
	decorators := append([]logHandlerDecorator(nil), handler.decorators...)
	decorators = append(decorators, decorator)
	return &reloadableLogHandler{state: handler.state, decorators: decorators}
}

func (handler *reloadableLogHandler) decorated(active slog.Handler) slog.Handler {
	for _, decorate := range handler.decorators {
		active = decorate(active)
	}
	return active
}

func (handler *reloadableLogHandler) replace(replacement loggingTarget) error {
	previous := handler.swap(replacement)
	if err := previous.destination.Close(); err != nil {
		return fmt.Errorf("close replaced log destination: %w", err)
	}
	return nil
}

func (handler *reloadableLogHandler) swap(replacement loggingTarget) loggingTarget {
	handler.state.mutex.Lock()
	previous := handler.state.target
	handler.state.target = replacement
	handler.state.mutex.Unlock()
	return previous
}

type noOpWriteCloser struct {
	writer io.Writer
}

func (destination noOpWriteCloser) Write(contents []byte) (int, error) {
	written, err := destination.writer.Write(contents)
	if err != nil {
		return written, fmt.Errorf("write log destination: %w", err)
	}
	return written, nil
}

func (noOpWriteCloser) Close() error {
	return nil
}
