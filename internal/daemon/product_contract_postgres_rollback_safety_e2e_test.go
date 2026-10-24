//go:build e2e

package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/core"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestProductContractPostgresRollbackRefusesActiveLeaderLease(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	services, target := preparePostgresRollback(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	elector := cluster.NewLeaderElector(services.connections)
	lease, err := elector.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("acquire PostgreSQL leader lease: %v", err)
	}
	t.Cleanup(func() {
		if lease == nil {
			return
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), e2eWaitLimit)
		defer closeCancel()
		if closeErr := lease.Close(closeCtx); closeErr != nil {
			t.Errorf("release PostgreSQL leader lease during cleanup: %v", closeErr)
		}
	})
	assertPostgresRollbackRefused(ctx, t, services, target, time.Now(), "leader lease is active")
	if err = lease.Close(ctx); err != nil {
		t.Fatalf("release PostgreSQL leader lease: %v", err)
	}
	lease = nil
	assertPostgresRollbackSucceeds(ctx, t, services, target, time.Now())
}

func TestProductContractPostgresRollbackRefusesLiveNonLeaderMember(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	services, target := preparePostgresRollback(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	heartbeatAt := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	publishPostgresMemberHeartbeat(ctx, t, services.connections, heartbeatAt)
	assertPostgresRollbackRefused(ctx, t, services, target, heartbeatAt, "cluster members are still live")
	expiredAt := heartbeatAt.Add(runtimeMemberTTL + time.Second)
	assertPostgresRollbackSucceeds(ctx, t, services, target, expiredAt)
}

func preparePostgresRollback(t *testing.T, fixture *e2ePostgresFixture) (e2ePostgresMigrationServices, int32) {
	t.Helper()
	services := newE2EPostgresMigrationServices(t, fixture.Settings())
	ctx, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	if err := services.migrator.Up(ctx); err != nil {
		t.Fatalf("migrate isolated PostgreSQL rollback database: %v", err)
	}
	status, err := services.migrator.Status(ctx)
	if err != nil || status.Current < 2 || status.Current != status.Target {
		t.Fatalf("PostgreSQL rollback preparation status = %+v, error = %v", status, err)
	}
	return services, status.Current - 1
}

func publishPostgresMemberHeartbeat(
	ctx context.Context,
	t *testing.T,
	connections *cluster.Connections,
	heartbeatAt time.Time,
) {
	t.Helper()
	pool, err := connections.OpenPool(ctx)
	if err != nil {
		t.Fatalf("open PostgreSQL state pool for member heartbeat: %v", err)
	}
	stateStore := postgresstate.NewConnectionStore(postgresstate.NewPGXConnection(pool))
	err = stateStore.PublishMemberState(ctx, core.MemberID("e2e-non-leader"), heartbeatAt, nil)
	pool.Close()
	if err != nil {
		t.Fatalf("publish PostgreSQL non-leader member heartbeat: %v", err)
	}
}

func assertPostgresRollbackRefused(
	ctx context.Context,
	t *testing.T,
	services e2ePostgresMigrationServices,
	target int32,
	now time.Time,
	wantError string,
) {
	t.Helper()
	err := services.migrator.RollbackTo(ctx, target, now.Add(-runtimeMemberTTL))
	if err == nil || !strings.Contains(err.Error(), wantError) {
		t.Fatalf("PostgreSQL rollback error = %v, want %q refusal", err, wantError)
	}
	assertPostgresMigrationVersion(ctx, t, services.migrator, target+1)
}

func assertPostgresRollbackSucceeds(
	ctx context.Context,
	t *testing.T,
	services e2ePostgresMigrationServices,
	target int32,
	now time.Time,
) {
	t.Helper()
	if err := services.migrator.RollbackTo(ctx, target, now.Add(-runtimeMemberTTL)); err != nil {
		t.Fatalf("roll back PostgreSQL after cluster evidence expires: %v", err)
	}
	assertPostgresMigrationVersion(ctx, t, services.migrator, target)
}

func assertPostgresMigrationVersion(ctx context.Context, t *testing.T, migrator *cluster.Migrator, want int32) {
	t.Helper()
	status, err := migrator.Status(ctx)
	if err != nil || status.Current != want {
		t.Fatalf("PostgreSQL migration version = %+v, error = %v, want %d", status, err, want)
	}
}
