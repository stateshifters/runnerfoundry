package main

import (
	"context"
	"errors"

	"github.com/stateshifters/runnerfoundry/internal/daemon"
)

type migrationStatus = daemon.MigrationStatus

type migrationCommandExecutor struct {
	status     func(context.Context, string) (daemon.MigrationStatus, error)
	up         func(context.Context, string) (daemon.MigrationStatus, error)
	rollbackTo func(context.Context, string, int32) (daemon.MigrationStatus, error)
}

func newProductionMigrationCommandExecutor() migrationCommandExecutor {
	application := daemon.NewMigrationApplication()
	return migrationCommandExecutor{
		status: application.Status, up: application.Up, rollbackTo: application.RollbackTo,
	}
}

func (executor migrationCommandExecutor) run(ctx context.Context, request migrationRequest) (migrationStatus, error) {
	switch request.action {
	case migrationStatusAction:
		return executor.status(ctx, request.configPath)
	case migrationUpAction:
		return executor.up(ctx, request.configPath)
	case migrationDownAction:
		return executor.rollbackTo(ctx, request.configPath, request.target)
	default:
		return migrationStatus{}, errors.New("unsupported migration command")
	}
}
