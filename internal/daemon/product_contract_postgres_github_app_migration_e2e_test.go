//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/cluster"
)

func TestPostgresGitHubAppMigrationUpDownUpRestoresOnlyLegacyRollbackEvidence(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	store := cluster.NewStore(fixture.Settings())
	ctx, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	if target := migratePostgresUp(ctx, t, store); target != 6 {
		t.Fatalf("PostgreSQL migration target = %d, want 6", target)
	}
	migratePostgresToVersionFive(ctx, t, store)
	populateLegacyPostgresGitHubState(ctx, t, store)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("apply PostgreSQL migration 006: %v", err)
	}
	assertPostgresAdapterRowCount(ctx, t, store, 0)
	migratePostgresToVersionFive(ctx, t, store)
	assertPostgresAdapterRowCount(ctx, t, store, 1)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("reapply PostgreSQL migration 006: %v", err)
	}
	assertPostgresAdapterRowCount(ctx, t, store, 0)
}

func migratePostgresToVersionFive(ctx context.Context, t *testing.T, store *cluster.Store) {
	t.Helper()
	if err := store.MigrateDown(ctx, 5, time.Now().Add(time.Hour), runtimeMemberTTL); err != nil {
		t.Fatalf("migrate PostgreSQL to schema 005: %v", err)
	}
}

func populateLegacyPostgresGitHubState(ctx context.Context, t *testing.T, store *cluster.Store) {
	t.Helper()
	pool, err := store.OpenPool(ctx)
	if err != nil {
		t.Fatalf("open PostgreSQL schema 005: %v", err)
	}
	defer pool.Close()
	for _, statement := range []string{
		`INSERT INTO public.runnerfoundry_github_registrations
			(repository_url, runner_pool, installation_id, runner_group_id, scale_set_id)
			VALUES ('https://github.example/legacy', 'linux', 1, 2, 3)`,
		`INSERT INTO public.runnerfoundry_github_scale_set_ownership
			(repository_url, runner_pool, installation_id, runner_group_id, scale_set_id)
			VALUES ('https://github.example/legacy', 'linux', 1, 2, 3)`,
	} {
		if _, err = pool.Exec(ctx, statement); err != nil {
			t.Fatalf("populate PostgreSQL schema 005: %v", err)
		}
	}
}

func assertPostgresAdapterRowCount(ctx context.Context, t *testing.T, store *cluster.Store, want int) {
	t.Helper()
	pool, err := store.OpenPool(ctx)
	if err != nil {
		t.Fatalf("open PostgreSQL adapter state: %v", err)
	}
	defer pool.Close()
	for _, table := range []string{"runnerfoundry_github_registrations", "runnerfoundry_github_scale_set_ownership"} {
		var count int
		if err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.`+table).Scan(&count); err != nil || count != want {
			t.Fatalf("PostgreSQL %s row count = %d, %v, want %d", table, count, err, want)
		}
	}
}
