package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestLoggingManagerReplacesTargetsAndPreservesDecorators(t *testing.T) {
	t.Parallel()

	initial := &memoryLogDestination{}
	replacement := &memoryLogDestination{}
	manager := memoryLoggingManager(t, initial, replacement)
	logger := manager.Logger().With("component", "runner").WithGroup("request").With("id", 42)
	logger.Debug("filtered")
	prepared, err := manager.Prepare(config.LogConfig{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatalf("prepare logging: %v", err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatalf("activate logging: %v", err)
	}
	logger.Debug("visible")
	if !strings.Contains(replacement.String(), `"component":"runner"`) || !strings.Contains(replacement.String(), `"request":{"id":42`) {
		t.Fatalf("replacement log = %q", replacement.String())
	}
}

func TestActivatedLoggingRejectsRepeatedActivationAndClosesManager(t *testing.T) {
	t.Parallel()
	manager := memoryLoggingManager(t, &memoryLogDestination{}, &memoryLogDestination{})
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err == nil {
		t.Fatal("prepared logging activated twice")
	}
	if err = prepared.Close(); err != nil {
		t.Fatalf("close activated preparation: %v", err)
	}
	if err = manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
}

func TestLoggingManagerSurfacesFactoryAndDestinationFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("destination failed")
	if _, err := newLoggingManager(config.LogConfig{}, testLogFactory(func(config.LogConfig) (io.WriteCloser, error) {
		return nil, wantErr
	})); !errors.Is(err, wantErr) {
		t.Fatalf("manager error = %v", err)
	}
	calls := 0
	manager, err := newLoggingManager(config.LogConfig{}, testLogFactory(func(config.LogConfig) (io.WriteCloser, error) {
		calls++
		if calls > 1 {
			return nil, wantErr
		}
		return &memoryLogDestination{closeErr: wantErr}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.Prepare(config.LogConfig{}); !errors.Is(err, wantErr) {
		t.Fatalf("prepare error = %v", err)
	}
	if err = manager.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v", err)
	}
}

func TestPreparedLoggingRetriesAbandonedTargetCleanupUntilItSucceeds(t *testing.T) {
	t.Parallel()

	initial := &memoryLogDestination{}
	candidate := &memoryLogDestination{closeErrors: []error{errors.New("close failed"), nil}}
	manager := memoryLoggingManager(t, initial, candidate)
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Close(); err == nil {
		t.Fatal("candidate close failure was ignored")
	}
	if err = prepared.Close(); err != nil || candidate.closes != 2 {
		t.Fatalf("second close = %v, calls=%d", err, candidate.closes)
	}
}

func TestPreparedLoggingClosesSuccessfulCandidate(t *testing.T) {
	t.Parallel()
	successful := &memoryLogDestination{}
	manager := memoryLoggingManager(t, &memoryLogDestination{}, successful)
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Close(); err != nil || successful.closes != 1 {
		t.Fatalf("successful candidate close = %v, calls=%d", err, successful.closes)
	}
}

func TestCommittedLoggingRetriesFailedDestinationCleanupUntilItSucceeds(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	initial := &memoryLogDestination{closeErrors: []error{wantErr, nil}}
	replacement := &memoryLogDestination{}
	manager := memoryLoggingManager(t, initial, replacement)
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Commit(); err != nil {
		t.Fatalf("commit replacement: %v", err)
	}
	if err = prepared.Close(); err != nil {
		t.Fatalf("retry committed cleanup: %v", err)
	}
	if err = prepared.Close(); err != nil || initial.closes != 2 || replacement.closes != 0 {
		t.Fatalf("idempotent close = %v, initial closes=%d replacement closes=%d", err, initial.closes, replacement.closes)
	}
}

func TestReloadableHandlerControlsEnabledAndHandleErrors(t *testing.T) {
	t.Parallel()

	handlerTarget := &controlledLogHandler{handleErr: errors.New("write failed")}
	handler := newReloadableLogHandler(loggingTarget{
		handler: handlerTarget, destination: noOpWriteCloser{writer: io.Discard},
	})
	record := slog.NewRecord(builderTime(), slog.LevelInfo, "message", 0)
	if handler.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("disabled handler was enabled")
	}
	if err := handler.Handle(context.Background(), record); err != nil || handlerTarget.handles != 0 {
		t.Fatalf("disabled handle = %v, calls=%d", err, handlerTarget.handles)
	}
	handlerTarget.enabled = true
	if err := handler.Handle(context.Background(), record); err == nil {
		t.Fatal("handler failure was ignored")
	}
	if handler.WithAttrs([]slog.Attr{slog.String("key", "value")}) == nil || handler.WithGroup("group") == nil {
		t.Fatal("decorated handler is nil")
	}
}

func TestNoOpWriteCloserDelegatesWrites(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	destination := noOpWriteCloser{writer: &output}
	if _, err := destination.Write([]byte("log")); err != nil || output.String() != "log" {
		t.Fatalf("write = %q, %v", output.String(), err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := (noOpWriteCloser{writer: errorLogWriter{}}).Write([]byte("log")); err == nil {
		t.Fatal("write failure was ignored")
	}
}

type memoryLogDestination struct {
	bytes.Buffer
	closeErr    error
	closeErrors []error
	closes      int
}

func (destination *memoryLogDestination) Close() error {
	destination.closes++
	if destination.closes <= len(destination.closeErrors) {
		return destination.closeErrors[destination.closes-1]
	}
	return destination.closeErr
}

type controlledLogHandler struct {
	enabled   bool
	handleErr error
	handles   int
}

func (handler *controlledLogHandler) Enabled(context.Context, slog.Level) bool {
	return handler.enabled
}
func (handler *controlledLogHandler) Handle(context.Context, slog.Record) error {
	handler.handles++
	return handler.handleErr
}
func (handler *controlledLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *controlledLogHandler) WithGroup(string) slog.Handler      { return handler }

type errorLogWriter struct{}

func (errorLogWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type testLogFactory func(config.LogConfig) (io.WriteCloser, error)

func (factory testLogFactory) Open(configured config.LogConfig) (io.WriteCloser, error) {
	return factory(configured)
}

func memoryLoggingManager(t *testing.T, destinations ...io.WriteCloser) *loggingManager {
	t.Helper()
	manager, err := newLoggingManager(config.LogConfig{}, testLogFactory(func(config.LogConfig) (io.WriteCloser, error) {
		destination := destinations[0]
		destinations = destinations[1:]
		return destination, nil
	}))
	if err != nil {
		t.Fatalf("create logging manager: %v", err)
	}
	return manager
}
