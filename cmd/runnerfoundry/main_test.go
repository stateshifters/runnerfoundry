package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/daemon"
)

func TestRunMainWritesExecutionErrorsToStderr(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error { return errors.New("daemon failed") }
	var stderr bytes.Buffer
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, &stderr, runDaemon, nil, testProcessLifecycle()); exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "daemon failed") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunMainWritesInvalidActiveRunnerPolicyDiagnosticSnapshot(t *testing.T) {
	var stderr bytes.Buffer
	exitCode := runMain(
		[]string{"--config", "config.yaml", "--active-runner-policy", "invalid"},
		io.Discard,
		&stderr,
		nil,
		nil,
		testProcessLifecycle(),
	)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	expected := "active runner policy must be leave or terminate\n"
	if stderr.String() != expected {
		t.Fatalf("stderr snapshot mismatch\nexpected: %q\nactual:   %q", expected, stderr.String())
	}
}

func TestRunMainWritesPostgreSQLMigrationDiagnosticSnapshot(t *testing.T) {
	runMigration := func(context.Context, migrationRequest) (migrationStatus, error) {
		return migrationStatus{}, errors.New(
			"apply PostgreSQL migrations: syntax error at or near \"INVALID\" (SQLSTATE 42601); " +
				"hint: Check the migration statement.",
		)
	}
	var stderr bytes.Buffer
	exitCode := runMain(
		[]string{"migrate", "up", "--config", "config.yaml"},
		io.Discard,
		&stderr,
		nil,
		runMigration,
		testProcessLifecycle(),
	)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	expected := "run PostgreSQL migration command: apply PostgreSQL migrations: " +
		"syntax error at or near \"INVALID\" (SQLSTATE 42601); hint: Check the migration statement.\n"
	if stderr.String() != expected {
		t.Fatalf("stderr snapshot mismatch\nexpected: %q\nactual:   %q", expected, stderr.String())
	}
}

func TestRunMainReturnsFailureWhenStderrCannotBeWritten(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error { return errors.New("daemon failed") }
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, failingWriter{}, runDaemon, nil, testProcessLifecycle()); exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestRunMainReturnsSuccess(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error { return nil }
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, io.Discard, runDaemon, nil, testProcessLifecycle()); exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
}

func TestRunProcessExitsWithCommandResult(t *testing.T) {
	exitCode := -1
	runtime := newProductionProcess()
	runtime.args = []string{"--unsupported"}
	runtime.stdout = io.Discard
	runtime.stderr = io.Discard
	runtime.lifecycle = testProcessLifecycle()
	runtime.exit = func(code int) { exitCode = code }
	runProcess(runtime)
	if exitCode != 1 {
		t.Fatalf("process exit code = %d, want 1", exitCode)
	}
}

func testProcessLifecycle() processLifecycle {
	return processLifecycle{
		notifyContext: func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
			return context.WithCancel(parent)
		},
		notifySignals: func(chan<- os.Signal, ...os.Signal) {},
		stopSignals:   func(chan<- os.Signal) {},
	}
}
