package main

import (
	"context"
	"fmt"
	"io"

	"github.com/meza/github-scaler/internal/daemon"
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
	if len(args) > 0 && args[0] == "migrate" {
		return executeMigrationCommand(ctx, args[1:], stdout, commandOutput, runMigration)
	}
	configPath, err := parseConfigPath(args, commandOutput)
	if err != nil {
		return err
	}
	if err := runDaemon(ctx, daemon.Options{ConfigPath: configPath, ReloadRequests: reloadRequests}); err != nil {
		return fmt.Errorf("run github scaler: %w", err)
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
	if request.action != migrationStatusAction {
		return nil
	}
	if _, err := fmt.Fprintf(stdout, "current=%d target=%d compatibility_floor=%d known=%d..%d\n",
		status.Current, status.Target, status.CompatibilityFloor, status.Minimum, status.Target); err != nil {
		return fmt.Errorf("write migration status: %w", err)
	}
	return nil
}
