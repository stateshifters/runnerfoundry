package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/meza/github-scaler/internal/daemon"
)

func TestRunMainWritesExecutionErrorsToStderr(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error { return errors.New("daemon failed") }
	var stderr bytes.Buffer
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, &stderr, runDaemon, nil); exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "daemon failed") {
		t.Fatalf("stderr = %q", stderr.String())
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
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, failingWriter{}, runDaemon, nil); exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestRunMainReturnsSuccess(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error { return nil }
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, io.Discard, runDaemon, nil); exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
}

func TestMainExitsWithCommandResult(t *testing.T) {
	originalArgs := os.Args
	originalExit := exitProcess
	t.Cleanup(func() {
		os.Args = originalArgs
		exitProcess = originalExit
	})
	os.Args = []string{"github-scaler", "--unsupported"}
	exitCode := -1
	exitProcess = func(code int) { exitCode = code }
	main()
	if exitCode != 1 {
		t.Fatalf("main exit code = %d, want 1", exitCode)
	}
}

func TestMainReportsMissingMigrationConfig(t *testing.T) {
	originalArgs := os.Args
	originalExit := exitProcess
	t.Cleanup(func() {
		os.Args = originalArgs
		exitProcess = originalExit
	})
	os.Args = []string{"github-scaler", "migrate", "status", "--config", "missing-config.yaml"}
	exitCode := -1
	exitProcess = func(code int) { exitCode = code }
	main()
	if exitCode != 1 {
		t.Fatalf("main exit code = %d, want 1", exitCode)
	}
}

func TestMainReportsInvalidPostgreSQLSettings(t *testing.T) {
	originalArgs := os.Args
	originalExit := exitProcess
	t.Cleanup(func() {
		os.Args = originalArgs
		exitProcess = originalExit
	})
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("state:\n  postgres:\n    role: github_scaler\n    port: invalid\n"), 0o600); err != nil {
		t.Fatalf("write migration config: %v", err)
	}
	os.Args = []string{"github-scaler", "migrate", "status", "--config", configPath}
	exitCode := -1
	exitProcess = func(code int) { exitCode = code }
	main()
	if exitCode != 1 {
		t.Fatalf("main exit code = %d, want 1", exitCode)
	}
}
