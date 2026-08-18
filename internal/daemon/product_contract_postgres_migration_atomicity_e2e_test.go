//go:build e2e

package daemon

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
)

func TestProductContractPostgresFailedMigrationsAreAtomic(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	ctx, cancel := context.WithTimeout(context.Background(), e2eWaitLimit)
	defer cancel()
	connection, err := fixture.e2ePostgresConnectAsApplication(ctx, fixture.Settings())
	if err != nil {
		t.Fatalf("open PostgreSQL atomicity migration connection: %v", err)
	}
	defer func() {
		if closeErr := connection.Close(context.WithoutCancel(ctx)); closeErr != nil {
			t.Errorf("close PostgreSQL atomicity migration connection: %v", closeErr)
		}
	}()

	migrator := newAtomicityMigrator(ctx, t, connection)
	if err = migrator.MigrateTo(ctx, 2); err != nil {
		t.Fatalf("establish PostgreSQL atomicity baseline: %v", err)
	}
	if err = migrator.MigrateTo(ctx, 3); err == nil {
		t.Fatal("failing PostgreSQL Up migration unexpectedly succeeded")
	}
	want := atomicityMigrationState{version: 2, payload: "transitioned", markerExists: true}
	assertAtomicityMigrationState(ctx, t, connection, migrator, want)
	if err = migrator.MigrateTo(ctx, 1); err == nil {
		t.Fatal("failing PostgreSQL Down migration unexpectedly succeeded")
	}
	assertAtomicityMigrationState(ctx, t, connection, migrator, want)
}

func newAtomicityMigrator(ctx context.Context, t *testing.T, connection *pgx.Conn) *migrate.Migrator {
	t.Helper()
	migrator, err := migrate.NewMigrator(ctx, connection, "public.runnerfoundry_atomicity_schema_version")
	if err != nil {
		t.Fatalf("create PostgreSQL atomicity migrator: %v", err)
	}
	migrator.AppendMigration("001_atomicity_baseline",
		`CREATE TABLE public.runnerfoundry_atomicity_probe (probe_id integer PRIMARY KEY, payload text NOT NULL);
		 INSERT INTO public.runnerfoundry_atomicity_probe (probe_id, payload) VALUES (1, 'baseline');`,
		`DROP TABLE public.runnerfoundry_atomicity_probe;`)
	migrator.AppendMigration("002_atomicity_transition",
		`ALTER TABLE public.runnerfoundry_atomicity_probe ADD COLUMN marker text NOT NULL DEFAULT 'present';
		 UPDATE public.runnerfoundry_atomicity_probe SET payload = 'transitioned';`,
		`UPDATE public.runnerfoundry_atomicity_probe SET payload = 'partially-down';
		 ALTER TABLE public.runnerfoundry_atomicity_probe DROP COLUMN marker;
		 SELECT 1 / 0;`)
	migrator.AppendMigration("003_atomicity_failure",
		`ALTER TABLE public.runnerfoundry_atomicity_probe ADD COLUMN leaked text;
		 UPDATE public.runnerfoundry_atomicity_probe SET payload = 'partially-up';
		 SELECT 1 / 0;`,
		`SELECT 1;`)
	return migrator
}

type atomicityMigrationState struct {
	version      int32
	payload      string
	markerExists bool
	leakedExists bool
}

func assertAtomicityMigrationState(
	ctx context.Context,
	t *testing.T,
	connection *pgx.Conn,
	migrator *migrate.Migrator,
	want atomicityMigrationState,
) {
	t.Helper()
	version, err := migrator.GetCurrentVersion(ctx)
	if err != nil || version != want.version {
		t.Fatalf("PostgreSQL version after failed migration = %d, error = %v, want %d", version, err, want.version)
	}
	var payload string
	var markerExists bool
	var leakedExists bool
	err = connection.QueryRow(ctx, `SELECT probe.payload,
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public'
			AND table_name = 'runnerfoundry_atomicity_probe' AND column_name = 'marker'),
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public'
			AND table_name = 'runnerfoundry_atomicity_probe' AND column_name = 'leaked')
		FROM public.runnerfoundry_atomicity_probe AS probe WHERE probe.probe_id = 1`).
		Scan(&payload, &markerExists, &leakedExists)
	if err != nil || payload != want.payload || markerExists != want.markerExists || leakedExists != want.leakedExists {
		t.Fatalf("PostgreSQL state after failed migration = payload %q marker=%t leaked=%t, error=%v; want %q, %t, %t",
			payload, markerExists, leakedExists, err, want.payload, want.markerExists, want.leakedExists)
	}
}
