//go:build integration

package cluster

import (
	"context"
	"testing"
	"time"
)

func TestPostgreSQLMigrationLifecycleIsReversible(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	now := time.Now()
	assertMigrationStatus(t, store, MigrationStatus{Current: 1, Target: 1, CompatibilityFloor: 1})
	var err error
	if err = store.Publish(ctx, "live-node", NodeHealthy, nil, now); err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateDown(ctx, 0, now, time.Minute); err == nil {
		t.Fatal("expected rollback to reject a live node")
	}
	if err = store.Publish(ctx, "live-node", NodeUnhealthy, nil, now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	lease, err := store.TryLeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateDown(ctx, 0, now, time.Minute); err == nil {
		t.Fatal("expected rollback to reject an active leader")
	}
	if err = lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateDown(ctx, 0, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	assertMigrationStatus(t, store, MigrationStatus{Target: 1})
	assertPostgreSQLDomainSchemaMissing(t, dsn)
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	assertPostgreSQLDomainSchemaExists(t, dsn)
}
