package main

import (
	"context"
	"errors"
	"testing"
)

func TestMigrationCommandDispatchesParsedRequestsToDaemonService(t *testing.T) {
	service := &migrationServiceFake{status: migrationStatus{Current: 3, Target: 8}}
	executor := migrationCommandExecutorForTest(service)

	requests := []migrationRequest{
		{action: migrationStatusAction, configPath: "status.yaml"},
		{action: migrationUpAction, configPath: "up.yaml"},
		{action: migrationDownAction, configPath: "down.yaml", target: 3},
	}
	for _, request := range requests {
		status, err := executor.run(t.Context(), request)
		if err != nil || status != service.status {
			t.Fatalf("execute %q = (%#v, %v)", request.action, status, err)
		}
	}
	if service.statusPath != "status.yaml" || service.upPath != "up.yaml" ||
		service.rollbackPath != "down.yaml" || service.rollbackTarget != 3 {
		t.Fatalf("migration service calls = %#v", service)
	}
}

func TestMigrationCommandReportsDaemonServiceFailures(t *testing.T) {
	failure := errors.New("failed")
	for name, request := range map[string]migrationRequest{
		"status": {action: migrationStatusAction},
		"up":     {action: migrationUpAction},
		"down":   {action: migrationDownAction},
	} {
		t.Run(name, func(t *testing.T) {
			service := &migrationServiceFake{err: failure}
			if _, err := migrationCommandExecutorForTest(service).run(t.Context(), request); !errors.Is(err, failure) {
				t.Fatalf("migration error = %v", err)
			}
		})
	}
}

func TestMigrationCommandRejectsUnsupportedAction(t *testing.T) {
	if _, err := (migrationCommandExecutor{}).run(t.Context(), migrationRequest{action: "unsupported"}); err == nil {
		t.Fatal("expected unsupported migration action")
	}
}

func TestProductionMigrationCommandConstructsWithoutConnecting(t *testing.T) {
	executor := newProductionMigrationCommandExecutor()
	if executor.status == nil || executor.up == nil || executor.rollbackTo == nil {
		t.Fatal("expected migration service")
	}
}

func migrationCommandExecutorForTest(service *migrationServiceFake) migrationCommandExecutor {
	return migrationCommandExecutor{
		status: service.Status, up: service.Up, rollbackTo: service.RollbackTo,
	}
}

type migrationServiceFake struct {
	status         migrationStatus
	err            error
	statusPath     string
	upPath         string
	rollbackPath   string
	rollbackTarget int32
}

func (service *migrationServiceFake) Status(_ context.Context, configPath string) (migrationStatus, error) {
	service.statusPath = configPath
	return service.status, service.err
}

func (service *migrationServiceFake) Up(_ context.Context, configPath string) (migrationStatus, error) {
	service.upPath = configPath
	return service.status, service.err
}

func (service *migrationServiceFake) RollbackTo(
	_ context.Context,
	configPath string,
	target int32,
) (migrationStatus, error) {
	service.rollbackPath = configPath
	service.rollbackTarget = target
	return service.status, service.err
}
