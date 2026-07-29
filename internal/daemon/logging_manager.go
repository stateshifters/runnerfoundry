package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/meza/RunnerFoundry/internal/config"
)

type logDestinationFactory interface {
	Open(logConfig config.LogConfig) (io.WriteCloser, error)
}

type logDestinationFactoryFunc func(config.LogConfig) (io.WriteCloser, error)

func (factory logDestinationFactoryFunc) Open(logConfig config.LogConfig) (io.WriteCloser, error) {
	return factory(logConfig)
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
	manager   *loggingManager
	target    loggingTarget
	mutex     sync.Mutex
	completed bool
}

func (prepared *preparedLogging) Activate() error {
	prepared.mutex.Lock()
	defer prepared.mutex.Unlock()
	if prepared.completed {
		return errors.New("prepared logging configuration is already completed")
	}
	prepared.completed = true
	return prepared.manager.handler.replace(prepared.target)
}

func (prepared *preparedLogging) Close() error {
	prepared.mutex.Lock()
	defer prepared.mutex.Unlock()
	if prepared.completed {
		return nil
	}
	prepared.completed = true
	if err := prepared.target.destination.Close(); err != nil {
		return fmt.Errorf("close prepared log destination: %w", err)
	}
	return nil
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
	handler.state.mutex.Lock()
	previous := handler.state.target
	handler.state.target = replacement
	handler.state.mutex.Unlock()
	if err := previous.destination.Close(); err != nil {
		return fmt.Errorf("close replaced log destination: %w", err)
	}
	return nil
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
