//go:build integration

package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLRunnerLifecycleMigrationBackfillsAndSurvivesDownUp(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 2, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(ctx, `INSERT INTO scaler_nodes(node_id, healthy, heartbeat_at)
		VALUES('spock', true, clock_timestamp());
		INSERT INTO scaler_libvirt_runner_lifecycle(node_id, scale_set, repository, request_id, runner_name, reap_requested)
		VALUES('spock', 'linux', 'repo', 42, 'linux-42', true),
		      ('spock', 'linux', 'repo', 43, 'linux-43', false)`); err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	assertMigratedRunnerLifecycle(t, dsn)
	if err = store.MigrateDown(ctx, 2, time.Now().Add(2*time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	assertMigratedRunnerLifecycle(t, dsn)
}

func TestPostgreSQLRunnerLifecycleDowngradeRejectsDockerCleanupObligation(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(ctx, `INSERT INTO scaler_nodes(node_id, healthy, heartbeat_at)
		VALUES('spock', true, clock_timestamp());
		INSERT INTO scaler_runner_lifecycle(
			node_id, scale_set, repository, provider, runner_name, request_id,
			phase, created_at, updated_at, reap_reason
		) VALUES('spock', 'linux', 'repo', 'docker', 'linux-1', 0,
			'reap_pending', clock_timestamp(), clock_timestamp(), 'job_start_timeout')`); err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	err = store.MigrateDown(ctx, 2, time.Now().Add(2*time.Minute), time.Minute)
	if err == nil || !strings.Contains(err.Error(), "Docker runner cleanup obligations remain") {
		t.Fatalf("downgrade error = %v", err)
	}
}

func assertMigratedRunnerLifecycle(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close(ctx) }()
	var provider, phase, reason string
	if err = connection.QueryRow(ctx, `SELECT provider, phase, reap_reason FROM scaler_runner_lifecycle
		WHERE node_id = 'spock' AND runner_name = 'linux-42'`).Scan(&provider, &phase, &reason); err != nil {
		t.Fatal(err)
	}
	if provider != "libvirt" || phase != "reap_pending" || reason != "legacy_reap_requested" {
		t.Fatalf("migrated lifecycle = provider %q phase %q reason %q", provider, phase, reason)
	}
	var waitingSince time.Time
	if err = connection.QueryRow(ctx, `SELECT phase, waiting_since FROM scaler_runner_lifecycle
		WHERE node_id = 'spock' AND runner_name = 'linux-43'`).Scan(&phase, &waitingSince); err != nil {
		t.Fatal(err)
	}
	if phase != "waiting_for_job" || waitingSince.IsZero() {
		t.Fatalf("waiting migration = phase %q waiting_since %v", phase, waitingSince)
	}
}
