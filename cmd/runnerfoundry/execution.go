package main

import (
	"context"
	"fmt"
	"io"

	"github.com/stateshifters/runnerfoundry/internal/buildinfo"
	"github.com/stateshifters/runnerfoundry/internal/daemon"
)

type daemonCommand func(context.Context, daemon.Options) error
type migrationCommand func(context.Context, migrationRequest) (migrationStatus, error)

func executeCommand(
	ctx context.Context,
	args []string,
	reloadRequests <-chan struct{},
	stdout io.Writer,
	commandOutput io.Writer,
	runDaemon daemonCommand,
	runMigration migrationCommand,
) error {
	if isVersionRequest(args) {
		return writeVersion(stdout)
	}
	if len(args) > 0 && args[0] == "migrate" {
		return executeMigrationCommand(ctx, args[1:], stdout, commandOutput, runMigration)
	}
	request, err := parseDaemonRequest(args, commandOutput)
	if err != nil {
		return err
	}
	if err := runDaemon(ctx, daemon.Options{
		ConfigPath:         request.configPath,
		ReloadRequests:     reloadRequests,
		ActiveRunnerPolicy: request.activeRunnerPolicy,
	}); err != nil {
		return fmt.Errorf("run RunnerFoundry: %w", err)
	}
	return nil
}

func isVersionRequest(args []string) bool {
	return len(args) == 1 && args[0] == "--version"
}

func writeVersion(output io.Writer) error {
	if _, err := fmt.Fprintf(output, "runnerfoundry %s\n", buildinfo.Version()); err != nil {
		return fmt.Errorf("write RunnerFoundry version: %w", err)
	}
	return nil
}

func executeMigrationCommand(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	commandOutput io.Writer,
	runMigration migrationCommand,
) error {
	request, err := parseMigrationRequest(args, commandOutput)
	if err != nil {
		return err
	}
	status, err := runMigration(ctx, request)
	if err != nil {
		return fmt.Errorf("run PostgreSQL migration command: %w", err)
	}
	if request.action == migrationUpAction || request.action == migrationDownAction {
		if _, err := fmt.Fprintf(stdout, "Migration %s completed at version %d.\n", request.action, status.Current); err != nil {
			return fmt.Errorf("write migration confirmation: %w", err)
		}
		return nil
	}
	if _, err := fmt.Fprintf(stdout, "current=%d target=%d known=%d..%d\n",
		status.Current, status.Target, status.Minimum, status.Target); err != nil {
		return fmt.Errorf("write migration status: %w", err)
	}
	return nil
}
