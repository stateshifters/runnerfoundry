package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/daemon"
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
	if capturedOptions.ConfigPath != "config.yaml" || capturedOptions.ReloadRequests != reloadRequests ||
		capturedOptions.ActiveRunnerPolicy != daemon.LeaveActiveRunners {
		t.Fatalf("daemon options = %#v", capturedOptions)
	}
}

func TestExecuteCommandPrintsVersionWithoutRunningServices(t *testing.T) {
	var stdout bytes.Buffer
	runDaemon := func(context.Context, daemon.Options) error {
		t.Fatal("daemon must not run for a version request")
		return nil
	}
	runMigration := func(context.Context, migrationRequest) (migrationStatus, error) {
		t.Fatal("migration service must not run for a version request")
		return migrationStatus{}, nil
	}
	if err := executeCommand(t.Context(), []string{"--version"}, nil,
		&stdout, io.Discard, runDaemon, runMigration); err != nil {
		t.Fatalf("print version: %v", err)
	}
	if stdout.String() != "runnerfoundry dev\n" {
		t.Fatalf("version snapshot mismatch: %q", stdout.String())
	}
}

func TestExecuteCommandReportsVersionOutputFailure(t *testing.T) {
	err := executeCommand(t.Context(), []string{"--version"}, nil,
		failingWriter{}, io.Discard, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "write RunnerFoundry version") {
		t.Fatalf("version output error = %v", err)
	}
}

func TestExecuteCommandPropagatesExplicitActiveRunnerPolicies(t *testing.T) {
	for _, policy := range []daemon.ActiveRunnerPolicy{daemon.LeaveActiveRunners, daemon.TerminateActiveRunners} {
		t.Run(string(policy), func(t *testing.T) {
			var capturedOptions daemon.Options
			runDaemon := func(_ context.Context, options daemon.Options) error {
				capturedOptions = options
				return nil
			}
			if err := executeCommand(t.Context(), []string{
				"--config", "config.yaml", "--active-runner-policy", string(policy),
			}, nil, io.Discard, io.Discard, runDaemon, nil); err != nil {
				t.Fatalf("run daemon: %v", err)
			}
			if capturedOptions.ActiveRunnerPolicy != policy {
				t.Fatalf("active runner policy = %q, want %q", capturedOptions.ActiveRunnerPolicy, policy)
			}
		})
	}
}

func TestExecuteCommandRejectsInvalidActiveRunnerPolicy(t *testing.T) {
	runDaemon := func(context.Context, daemon.Options) error {
		t.Fatal("daemon must not run for an invalid active runner policy")
		return nil
	}
	err := executeCommand(t.Context(), []string{
		"--config", "config.yaml", "--active-runner-policy", "invalid",
	}, nil, io.Discard, io.Discard, runDaemon, nil)
	if err == nil || !strings.Contains(err.Error(), "active runner policy") {
		t.Fatalf("invalid policy error = %v", err)
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
		return migrationStatus{Current: 1, Target: 2, Minimum: 0}, nil
	}
	var stdout bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "status", "--config", "config.yaml"}, nil,
		&stdout, io.Discard, nil, runMigration); err != nil {
		t.Fatalf("run migration status: %v", err)
	}
	if capturedRequest.action != migrationStatusAction || capturedRequest.configPath != "config.yaml" {
		t.Fatalf("migration request = %#v", capturedRequest)
	}
	if stdout.String() != "current=1 target=2 known=0..2\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestMigrationUpPrintsCompletion(t *testing.T) {
	var captured migrationRequest
	runMigration := func(_ context.Context, request migrationRequest) (migrationStatus, error) {
		captured = request
		return migrationStatus{Current: 2}, nil
	}
	var stdout bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "up", "--config", "config.yaml"}, nil,
		&stdout, io.Discard, nil, runMigration); err != nil {
		t.Fatal(err)
	}
	if captured.action != migrationUpAction || stdout.String() != "Migration up completed at version 2.\n" {
		t.Fatalf("migration request = %#v, stdout = %q", captured, stdout.String())
	}
}

func TestMigrationDownPrintsCompletion(t *testing.T) {
	var captured migrationRequest
	runMigration := func(_ context.Context, request migrationRequest) (migrationStatus, error) {
		captured = request
		return migrationStatus{Current: 1}, nil
	}
	var stdout bytes.Buffer
	if err := executeCommand(t.Context(), []string{"migrate", "down", "--to", "1", "--config", "config.yaml"}, nil,
		&stdout, io.Discard, nil, runMigration); err != nil {
		t.Fatal(err)
	}
	if captured.action != migrationDownAction || captured.target != 1 ||
		stdout.String() != "Migration down completed at version 1.\n" {
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

func TestMigrationCompletionReportsOutputFailure(t *testing.T) {
	runMigration := func(context.Context, migrationRequest) (migrationStatus, error) {
		return migrationStatus{Current: 1}, nil
	}
	err := executeCommand(t.Context(), []string{"migrate", "up", "--config", "config.yaml"}, nil,
		failingWriter{}, io.Discard, nil, runMigration)
	if err == nil || !strings.Contains(err.Error(), "write migration confirmation") {
		t.Fatalf("migration completion output error = %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
