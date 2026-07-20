package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/meza/github-scaler/internal/daemon"
)

func TestExecuteCommandRunsDaemonWithConfigAndReloadRequests(t *testing.T) {
	var capturedOptions daemon.Options
	runDaemon := func(_ context.Context, options daemon.Options) error {
		capturedOptions = options
		return nil
	}
	reloadRequests := make(chan struct{})
	if err := executeCommand(t.Context(), []string{"--config", "config.yaml"}, reloadRequests,
		io.Discard, io.Discard, runDaemon, nil); err != nil {
		t.Fatalf("run daemon: %v", err)
	}
	if capturedOptions.ConfigPath != "config.yaml" || capturedOptions.ReloadRequests != reloadRequests {
		t.Fatalf("daemon options = %#v", capturedOptions)
	}
}

func TestExecuteCommandReportsDaemonFailure(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error { return errors.New("daemon failed") }
	err := executeCommand(t.Context(), []string{"--config", "config.yaml"}, nil,
		io.Discard, io.Discard, runDaemon, nil)
	if err == nil || !strings.Contains(err.Error(), "daemon failed") {
		t.Fatalf("daemon error = %v", err)
	}
}

func TestMigrationStatusWritesMachineReadableStdout(t *testing.T) {
	var capturedRequest migrationRequest
	runMigration := func(_ context.Context, request migrationRequest) (migrationStatus, error) {
		capturedRequest = request
		return migrationStatus{Current: 1, Target: 2, CompatibilityFloor: 1, Minimum: 0}, nil
	}
	var stdout bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "status", "--config", "config.yaml"}, nil,
		&stdout, io.Discard, nil, runMigration); err != nil {
		t.Fatalf("run migration status: %v", err)
	}
	if capturedRequest.action != migrationStatusAction || capturedRequest.configPath != "config.yaml" {
		t.Fatalf("migration request = %#v", capturedRequest)
	}
	if stdout.String() != "current=1 target=2 compatibility_floor=1 known=0..2\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestMigrationUpRemainsSilent(t *testing.T) {
	var captured migrationRequest
	runMigration := func(_ context.Context, request migrationRequest) (migrationStatus, error) {
		captured = request
		return migrationStatus{}, nil
	}
	var stdout bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "up", "--config", "config.yaml"}, nil,
		&stdout, io.Discard, nil, runMigration); err != nil {
		t.Fatal(err)
	}
	if captured.action != migrationUpAction || stdout.Len() != 0 {
		t.Fatalf("migration request = %#v, stdout = %q", captured, stdout.String())
	}
}

func TestMigrationDownPassesTargetAndRemainsSilent(t *testing.T) {
	var captured migrationRequest
	runMigration := func(_ context.Context, request migrationRequest) (migrationStatus, error) {
		captured = request
		return migrationStatus{}, nil
	}
	var stdout bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "down", "--to", "1", "--config", "config.yaml"}, nil,
		&stdout, io.Discard, nil, runMigration); err != nil {
		t.Fatal(err)
	}
	if captured.action != migrationDownAction || captured.target != 1 || stdout.Len() != 0 {
		t.Fatalf("migration request = %#v, stdout = %q", captured, stdout.String())
	}
}

func TestMigrationCommandReportsServiceFailure(t *testing.T) {
	runMigration := func(context.Context, migrationRequest) (migrationStatus, error) {
		return migrationStatus{}, errors.New("migration failed")
	}
	err := executeCommand(t.Context(), []string{"migrate", "up", "--config", "config.yaml"}, nil,
		io.Discard, io.Discard, nil, runMigration)
	if err == nil || !strings.Contains(err.Error(), "migration failed") {
		t.Fatalf("migration error = %v", err)
	}
}

func TestMigrationStatusReportsOutputFailure(t *testing.T) {
	runMigration := func(context.Context, migrationRequest) (migrationStatus, error) {
		return migrationStatus{Current: 1, Target: 1}, nil
	}
	if err := executeCommand(t.Context(), []string{"migrate", "status", "--config", "config.yaml"}, nil,
		failingWriter{}, io.Discard, nil, runMigration); err == nil {
		t.Fatal("expected status output failure")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
