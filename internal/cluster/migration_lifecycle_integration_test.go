//go:build integration

package cluster

import (
	"context"
	"testing"
	"time"
)

func TestPostgreSQLMigrationLifecycleIsReversible(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	now := time.Now()
	assertMigrationStatus(t, store, MigrationStatus{Current: 2, Target: 2, CompatibilityFloor: 1})
	if err := store.MigrateDown(context.Background(), 0, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	assertMigrationStatus(t, store, MigrationStatus{Target: 2})
	assertPostgreSQLDomainSchemaMissing(t, dsn)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertPostgreSQLDomainSchemaExists(t, dsn)
}

func TestPostgreSQLRollbackRejectsLiveNode(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	now := time.Now()
	if err := store.Publish(context.Background(), "live-node", NodeHealthy, nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateDown(context.Background(), 0, now, time.Minute); err == nil {
		t.Fatal("expected rollback to reject a live node")
	}
}

func TestPostgreSQLRollbackRejectsActiveLeader(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	lease := acquireLeader(t, store)
	t.Cleanup(func() {
		if err := lease.Close(context.Background()); err != nil {
			t.Errorf("close leader lease: %v", err)
		}
	})
	if err := store.MigrateDown(context.Background(), 0, time.Now(), time.Minute); err == nil {
		t.Fatal("expected rollback to reject an active leader")
	}
}
