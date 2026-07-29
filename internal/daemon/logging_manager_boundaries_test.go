package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestLoggingStandaloneReloadReportsReplacementPreparationFailure(t *testing.T) {
	initial := &recordingLogDestination{}
	factoryCalls := 0
	manager, err := newLoggingManager(config.LogConfig{}, logDestinationFactoryFunc(func(config.LogConfig) (io.WriteCloser, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return initial, nil
		}
		return nil, errors.New("replacement unavailable")
	}))
	if err != nil {
		t.Fatal(err)
	}
	loadedConfig := mockedStandaloneLoggingConfig()
	active := testActiveConfiguration()
	active.Config.State.Path = loadedConfig.State.Path
	active.Logger = manager.Logger()
	active.Logging = manager
	if _, err = reloadConfiguration(context.Background(), "config", successfulReloadDaemon(loadedConfig), active); err == nil ||
		!strings.Contains(err.Error(), "prepare reloaded logging") {
		t.Fatalf("reload error = %v", err)
	}
	if initial.closeCalls != 0 {
		t.Fatalf("active destination close calls = %d", initial.closeCalls)
	}
}

func TestLoggingStandaloneReloadReportsActivationFailure(t *testing.T) {
	current := &recordingLogDestination{closeErr: errors.New("current close failed")}
	manager := newTestLoggingManager(t, current, &recordingLogDestination{})
	loadedConfig := mockedStandaloneLoggingConfig()
	active := testActiveConfiguration()
	active.Config.State.Path = loadedConfig.State.Path
	active.Logger = manager.Logger()
	active.Logging = manager
	_, err := reloadConfiguration(context.Background(), "config", successfulReloadDaemon(loadedConfig), active)
	if err == nil || !strings.Contains(err.Error(), "activate reloaded logging") {
		t.Fatalf("reload error = %v", err)
	}
}

func TestStandaloneStartupReportsLogDestinationFailure(t *testing.T) {
	daemon := successfulReloadDaemon(mockedStandaloneLoggingConfig())
	daemon.newLogging = func(config.LogConfig) (*loggingManager, error) {
		return nil, errors.New("log destination unavailable")
	}
	if _, err := loadConfiguration("config", daemon); err == nil || !strings.Contains(err.Error(), "open log destination") {
		t.Fatalf("startup error = %v", err)
	}
}

func TestClosePreparedLoggingClearsCandidateAfterCloseFailure(t *testing.T) {
	manager := newTestLoggingManager(t, &recordingLogDestination{}, &recordingLogDestination{closeErr: errors.New("close failed")})
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	configuration := activeConfiguration{PreparedLogging: prepared}
	if err = closePreparedLogging(&configuration); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("close error = %v", err)
	}
	if configuration.PreparedLogging != nil {
		t.Fatal("failed candidate close remained pending")
	}
	if err = closePreparedLogging(&configuration); err != nil {
		t.Fatalf("repeated candidate cleanup: %v", err)
	}
}

func TestLoggingManagerReportsInitialDestinationFailure(t *testing.T) {
	_, err := newLoggingManager(config.LogConfig{}, logDestinationFactoryFunc(func(config.LogConfig) (io.WriteCloser, error) {
		return nil, errors.New("destination unavailable")
	}))
	if err == nil || !strings.Contains(err.Error(), "destination unavailable") {
		t.Fatalf("initialization error = %v", err)
	}
}

func TestActivatedLoggingCannotBeActivatedAgain(t *testing.T) {
	manager := newTestLoggingManager(t, &recordingLogDestination{}, &recordingLogDestination{})
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err == nil || !strings.Contains(err.Error(), "already completed") {
		t.Fatalf("second activation error = %v", err)
	}
	if err = prepared.Close(); err != nil {
		t.Fatalf("close after activation: %v", err)
	}
}

func TestAbandonedLoggingDestinationIsClosedExactlyOnce(t *testing.T) {
	candidate := &recordingLogDestination{closeErr: errors.New("candidate close failed")}
	manager := newTestLoggingManager(t, &recordingLogDestination{}, candidate)
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Close(); err == nil || !strings.Contains(err.Error(), "candidate close failed") {
		t.Fatalf("candidate close error = %v", err)
	}
	if err = prepared.Close(); err != nil || candidate.closeCalls != 1 {
		t.Fatalf("second close error=%v close calls=%d", err, candidate.closeCalls)
	}
}

func TestReloadableLogHandlerPreservesAttributesGroupsAndLevelAcrossReplacement(t *testing.T) {
	initial := &recordingLogDestination{}
	replacement := &recordingLogDestination{}
	manager := newTestLoggingManager(t, initial, replacement)
	logger := manager.Logger().With("component", "runner").WithGroup("request").With("id", 42)
	logger.Debug("filtered before replacement")
	prepared, err := manager.Prepare(config.LogConfig{Format: "json", Level: "debug"})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	logger.Debug("decorated after replacement")
	if strings.Contains(initial.String(), "filtered before replacement") {
		t.Fatalf("debug record bypassed initial info level: %q", initial.String())
	}
	for _, expected := range []string{`"component":"runner"`, `"request":{"id":42`, `"msg":"decorated after replacement"`} {
		if !strings.Contains(replacement.String(), expected) {
			t.Fatalf("replacement log %q does not contain %q", replacement.String(), expected)
		}
	}
}

func TestReloadableLogHandlerReportsHandlerFailureAndSkipsDisabledRecords(t *testing.T) {
	target := &controllableLogHandler{handleErr: errors.New("write failed")}
	handler := newReloadableLogHandler(loggingTarget{handler: target, destination: noOpWriteCloser{writer: io.Discard}})
	record := slog.NewRecord(time.Time{}, slog.LevelInfo, "message", 0)
	if err := handler.Handle(context.Background(), record); err != nil || target.handleCalls != 0 {
		t.Fatalf("disabled handle error=%v calls=%d", err, target.handleCalls)
	}
	target.enabled = true
	if err := handler.Handle(context.Background(), record); err == nil || !strings.Contains(err.Error(), "handle log record") {
		t.Fatalf("enabled handle error = %v", err)
	}
}

func TestLoggingManagerAndDestinationSurfaceCloseAndWriteFailures(t *testing.T) {
	manager := newTestLoggingManager(t, &recordingLogDestination{closeErr: errors.New("active close failed")})
	if err := manager.Close(); err == nil || !strings.Contains(err.Error(), "active close failed") {
		t.Fatalf("manager close error = %v", err)
	}
	destination := noOpWriteCloser{writer: errorWriter{err: errors.New("sink failed")}}
	if _, err := destination.Write([]byte("log")); err == nil || !strings.Contains(err.Error(), "write log destination") {
		t.Fatalf("destination write error = %v", err)
	}
}

type errorWriter struct{ err error }

func (writer errorWriter) Write([]byte) (int, error) { return 0, writer.err }

type controllableLogHandler struct {
	enabled     bool
	handleErr   error
	handleCalls int
}

func (handler *controllableLogHandler) Enabled(context.Context, slog.Level) bool {
	return handler.enabled
}
func (handler *controllableLogHandler) Handle(context.Context, slog.Record) error {
	handler.handleCalls++
	return handler.handleErr
}
func (handler *controllableLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *controllableLogHandler) WithGroup(string) slog.Handler      { return handler }
