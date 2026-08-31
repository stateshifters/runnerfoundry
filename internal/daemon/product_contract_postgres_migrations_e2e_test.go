//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/core"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestProductContractPostgresMigrationsRoundTripOnIsolatedDatabase(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	store := cluster.NewStore(fixture.Settings())
	ctx, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	target := migratePostgresUp(ctx, t, store)
	populateCurrentPostgresState(ctx, t, store)
	if err := fixture.e2ePostgresAssertStableRoleOwnership(ctx); err != nil {
		t.Fatalf("verify migrated PostgreSQL stable-role ownership: %v", err)
	}
	migratePostgresDown(ctx, t, store, target)
	assertVersionOnePostgresState(ctx, t, store)
	remigratePostgres(ctx, t, store, target)
	assertMigratedPostgresState(ctx, t, store)
	if err := fixture.e2ePostgresAssertStableRoleOwnership(ctx); err != nil {
		t.Fatalf("verify remigrated PostgreSQL stable-role ownership: %v", err)
	}
}

func migratePostgresUp(ctx context.Context, t *testing.T, store *cluster.Store) int32 {
	t.Helper()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate isolated PostgreSQL database up: %v", err)
	}
	upStatus, err := store.MigrationStatus(ctx)
	if err != nil || upStatus.Current == 0 || upStatus.Current != upStatus.Target {
		t.Fatalf("PostgreSQL migration status after up = %+v, error = %v", upStatus, err)
	}
	return upStatus.Target
}

func migratePostgresDown(ctx context.Context, t *testing.T, store *cluster.Store, target int32) {
	t.Helper()
	if err := store.MigrateDown(ctx, 1, time.Now().Add(time.Hour), runtimeMemberTTL); err != nil {
		t.Fatalf("migrate isolated PostgreSQL database down: %v", err)
	}
	downStatus, err := store.MigrationStatus(ctx)
	if err != nil || downStatus.Current != 1 || downStatus.Target != target {
		t.Fatalf("PostgreSQL migration status after down = %+v, error = %v", downStatus, err)
	}
}

func assertVersionOnePostgresState(ctx context.Context, t *testing.T, clusterStore *cluster.Store) {
	t.Helper()
	pool, err := clusterStore.OpenPool(ctx)
	if err != nil {
		t.Fatalf("open rolled-back PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	assertRolledBackPostgresSchema(ctx, t, pool)
	assertVersionOnePostgresDurableState(ctx, t, pool)
}

func assertRolledBackPostgresSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	assertRolledBackPostgresRelations(ctx, t, pool)
	assertRolledBackPostgresRunnerName(ctx, t, pool)
	assertRolledBackPostgresNotificationFunction(ctx, t, pool)
	assertRolledBackPostgresNotificationTrigger(ctx, t, pool)
}

func assertRolledBackPostgresRelations(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, objectName := range []string{
		"runnerfoundry_github_scale_set_ownership",
		"runnerfoundry_github_member_runner_pools",
	} {
		if postgresRelationExists(ctx, t, pool, objectName) {
			t.Fatalf("rolled-back PostgreSQL object public.%s still exists", objectName)
		}
	}
}

func assertRolledBackPostgresRunnerName(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var runnerNameColumnExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'runnerfoundry_processing_jobs' AND column_name = 'runner_name'
	)`).Scan(&runnerNameColumnExists); err != nil {
		t.Fatalf("inspect rolled-back runner_name column: %v", err)
	}
	if runnerNameColumnExists {
		t.Fatal("rolled-back runner_name column still exists")
	}
}

func assertRolledBackPostgresNotificationFunction(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var notificationFunctionExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regprocedure('public.runnerfoundry_notify_queued_job()') IS NOT NULL`).
		Scan(&notificationFunctionExists); err != nil {
		t.Fatalf("inspect rolled-back notification function: %v", err)
	}
	if notificationFunctionExists {
		t.Fatal("rolled-back notification function still exists")
	}
}

func assertRolledBackPostgresNotificationTrigger(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var notificationTriggerExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_catalog.pg_trigger
		WHERE tgname = 'runnerfoundry_queued_jobs_notify' AND NOT tgisinternal
	)`).Scan(&notificationTriggerExists); err != nil {
		t.Fatalf("inspect rolled-back notification trigger: %v", err)
	}
	if notificationTriggerExists {
		t.Fatal("rolled-back notification trigger still exists")
	}
}

func postgresRelationExists(ctx context.Context, t *testing.T, pool *pgxpool.Pool, objectName string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, objectName).Scan(&exists); err != nil {
		t.Fatalf("inspect rolled-back PostgreSQL object %s: %v", objectName, err)
	}
	return exists
}

func assertVersionOnePostgresDurableState(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	store := postgresstate.NewConnectionStore(postgresstate.NewPGXConnection(pool))
	assertMigratedQueueClassifications(ctx, t, store, core.QueueProcessing)
	var registrations int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.runnerfoundry_github_registrations`).Scan(&registrations); err != nil {
		t.Fatalf("count version-one registrations after rollback: %v", err)
	}
	if registrations != 0 {
		t.Fatalf("version-one registrations after rollback = %d, want 0", registrations)
	}
	capacity, capacityErr := store.AvailableRunnerPoolCapacity(
		ctx, "linux", time.Date(2026, time.August, 16, 11, 59, 30, 0, time.UTC),
	)
	if capacityErr != nil || capacity != 1 {
		t.Fatalf("version-one available capacity after rollback = %d, error = %v, want 1", capacity, capacityErr)
	}
}

func assertMigratedPostgresState(
	ctx context.Context,
	t *testing.T,
	clusterStore *cluster.Store,
) {
	t.Helper()
	pool, err := clusterStore.OpenPool(ctx)
	if err != nil {
		t.Fatalf("open migrated PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := postgresstate.NewConnectionStore(postgresstate.NewPGXConnection(pool))
	assertMigratedQueueClassifications(ctx, t, store, core.QueueQueued)
	assertRemigratedGitHubState(ctx, t, store)
	assertRemigratedCapacity(ctx, t, store)
}

func assertRemigratedGitHubState(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
) {
	t.Helper()
	registrations, registrationErr := store.Registrations(ctx)
	owned, ownershipErr := store.OwnedRegistrations(ctx)
	if registrationErr != nil || ownershipErr != nil || len(registrations) != 0 || len(owned) != 0 {
		t.Fatalf("migrated registrations = %+v, owned = %+v, errors = (%v, %v)", registrations, owned, registrationErr, ownershipErr)
	}
}

func assertRemigratedCapacity(ctx context.Context, t *testing.T, store *postgresstate.Store) {
	t.Helper()
	capacity, err := store.AvailableRunnerPoolCapacity(ctx, "linux", time.Date(2026, time.August, 16, 11, 59, 30, 0, time.UTC))
	if err != nil || capacity != 1 {
		t.Fatalf("migrated available capacity = %d, error = %v, want 1", capacity, err)
	}
	declarations, err := store.LiveGitHubRunnerPoolDeclarations(ctx, time.Date(2026, time.August, 16, 11, 59, 30, 0, time.UTC))
	if err != nil || len(declarations) != 0 {
		t.Fatalf("remigrated declarations = %+v, error = %v, want none after documented rollback loss", declarations, err)
	}
}

func assertMigratedQueueClassifications(
	ctx context.Context,
	t *testing.T,
	store *postgresstate.Store,
	processingOutcome core.QueueOutcome,
) {
	t.Helper()
	for sourceID, want := range map[string]core.QueueOutcome{
		"upgrade-queued": core.QueueAlreadyQueued, "upgrade-processing": processingOutcome,
		"upgrade-terminal": core.QueueSourceTerminal,
	} {
		result, err := store.Queue(ctx, core.JobRequest{
			Source:     core.SourceReference{Adapter: "e2e", ID: core.SourceIdentifier(sourceID)},
			RunnerPool: "linux", EnqueuedAt: time.Now(),
		})
		if err != nil || result.Outcome != want {
			t.Fatalf("migrated %s classification = %+v, error = %v, want %s", sourceID, result, err, want)
		}
	}
}

func remigratePostgres(ctx context.Context, t *testing.T, store *cluster.Store, target int32) {
	t.Helper()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("remigrate isolated PostgreSQL database: %v", err)
	}
	remigrated, err := store.MigrationStatus(ctx)
	if err != nil || remigrated.Current != target {
		t.Fatalf("PostgreSQL migration status after remigration = %+v, error = %v", remigrated, err)
	}
}
