package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLCoordinatesMultiHostScaleSets(t *testing.T) { //nolint:gocyclo // One end-to-end scenario intentionally verifies the complete cluster invariant set.
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	liveFor := time.Minute

	publish := func(nodeID string, healthy bool, contributions ...Contribution) {
		t.Helper()
		if err := store.Publish(ctx, nodeID, healthy, contributions, now); err != nil {
			t.Fatal(err)
		}
	}
	publish("spock", true,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 10, Healthy: true},
		Contribution{ScaleSet: "spock-only", RunnerGroup: "Default", MaxRunners: 3, Healthy: true},
	)
	publish("serenity", true,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 2, Healthy: true},
		Contribution{ScaleSet: "serenity-only", RunnerGroup: "Default", MaxRunners: 4, Healthy: true},
	)
	assertScaleSetStatus(t, store, now, liveFor, map[string]ScaleSetStatus{
		"meza-ubuntu-slim": {Name: "meza-ubuntu-slim", RunnerGroup: "Default", EffectiveCapacity: 12, Compatible: true},
		"serenity-only":    {Name: "serenity-only", RunnerGroup: "Default", EffectiveCapacity: 4, Compatible: true},
		"spock-only":       {Name: "spock-only", RunnerGroup: "Default", EffectiveCapacity: 3, Compatible: true},
	})
	if err := store.Enqueue(ctx, Demand{
		ScaleSet: "serenity-only", Repository: "repo", RequestID: 100, QueueTime: now,
	}); err != nil {
		t.Fatal(err)
	}
	parsedDSN, parseErr := url.Parse(dsn)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	credentialDirectory := t.TempDir()
	userPath := filepath.Join(credentialDirectory, "postgres-user")
	passwordPath := filepath.Join(credentialDirectory, "postgres-password")
	for path, contents := range map[string]string{userPath: "postgres\n", passwordPath: "postgres\n"} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fileBackedStore := NewStore(ConnectionSettings{
		Host: "localhost", Port: parsedDSN.Port(), Name: "postgres",
		User: credentialFileURL(userPath), Password: credentialFileURL(passwordPath), Role: "postgres",
	})
	if statuses := mustScaleSets(t, fileBackedStore, now, liveFor); len(statuses) != 3 {
		t.Fatalf("file-backed authentication did not read clustered state: %#v", statuses)
	}
	if err := os.WriteFile(passwordPath, []byte("rotated-but-not-yet-valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fileBackedStore.ScaleSets(ctx, now, liveFor); err == nil {
		t.Fatal("expected the next connection to read the changed password file")
	}
	if err := os.WriteFile(passwordPath, []byte("postgres\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if statuses := mustScaleSets(t, fileBackedStore, now, liveFor); len(statuses) != 3 {
		t.Fatalf("restored file-backed authentication did not reconnect: %#v", statuses)
	}

	for requestID := int64(1); requestID <= 13; requestID++ {
		repository := "https://github.com/example/alpha"
		if requestID%2 == 0 {
			repository = "https://github.com/example/beta"
		}
		if err := store.Enqueue(ctx, Demand{
			ScaleSet: "meza-ubuntu-slim", Repository: repository, RequestID: requestID,
			QueueTime: now.Add(time.Duration(requestID) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	assignmentsByNode := map[string]int{}
	var firstSpockAssignment *Assignment
	for requestID := int64(1); requestID <= 12; requestID++ {
		assignment, err := store.AssignOldest(ctx, "meza-ubuntu-slim", now, liveFor)
		if err != nil {
			t.Fatal(err)
		}
		if assignment == nil || assignment.RequestID != requestID {
			t.Fatalf("assignment %d = %#v; global FIFO was not preserved", requestID, assignment)
		}
		assignmentsByNode[assignment.NodeID]++
		if assignment.NodeID == "spock" && firstSpockAssignment == nil {
			firstSpockAssignment = assignment
		}
	}
	if assignmentsByNode["spock"] != 10 || assignmentsByNode["serenity"] != 2 {
		t.Fatalf("local limits exceeded or underused: %#v", assignmentsByNode)
	}
	assignment, assignErr := store.AssignOldest(ctx, "meza-ubuntu-slim", now, liveFor)
	if !errors.Is(assignErr, ErrNoAssignment) || assignment != nil {
		t.Fatalf("expected demand to remain queued after aggregate capacity was full, got %#v, %v", assignment, assignErr)
	}
	if readyErr := store.Ready(ctx, firstSpockAssignment.ID, 42, 99); readyErr != nil {
		t.Fatal(readyErr)
	}
	ready, err := store.ClaimReady(ctx, "spock")
	if err != nil {
		t.Fatal(err)
	}
	if ready == nil || ready.RequestID != firstSpockAssignment.RequestID || ready.InstallationID != 42 || ready.GitHubScaleSetID != 99 {
		t.Fatalf("ready assignment lost execution metadata: %#v", ready)
	}
	if startedErr := store.Started(ctx, ready.ID); startedErr != nil {
		t.Fatal(startedErr)
	}
	if removeErr := store.RemoveDemand(ctx, ready.ScaleSet, ready.Repository, ready.RequestID); removeErr != nil {
		t.Fatal(removeErr)
	}
	assignment, err = store.AssignOldest(ctx, "meza-ubuntu-slim", now, liveFor)
	if err != nil {
		t.Fatal(err)
	}
	if assignment == nil || assignment.RequestID != 13 || assignment.NodeID != "spock" {
		t.Fatalf("released capacity did not admit the next FIFO demand: %#v", assignment)
	}

	publish("serenity", false,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 2, Healthy: true},
		Contribution{ScaleSet: "serenity-only", RunnerGroup: "Default", MaxRunners: 4, Healthy: true},
	)
	statuses, err := store.ScaleSets(ctx, now, liveFor)
	if err != nil {
		t.Fatal(err)
	}
	if findScaleSet(t, statuses, "meza-ubuntu-slim").EffectiveCapacity != 10 {
		t.Fatalf("unhealthy node contribution was not removed: %#v", statuses)
	}

	publish("serenity", true,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Other", MaxRunners: 2, Healthy: true},
	)
	conflicted := findScaleSet(t, mustScaleSets(t, store, now, liveFor), "meza-ubuntu-slim")
	if conflicted.Compatible || conflicted.EffectiveCapacity != 0 {
		t.Fatalf("conflicting declarations contributed capacity: %#v", conflicted)
	}

	publish("serenity", true)
	statuses = mustScaleSets(t, store, now, liveFor)
	if findScaleSet(t, statuses, "meza-ubuntu-slim").EffectiveCapacity != 10 {
		t.Fatalf("explicit withdrawal did not preserve remaining contribution: %#v", statuses)
	}
	if hasScaleSet(statuses, "serenity-only") {
		t.Fatalf("last explicit withdrawal did not remove scale set: %#v", statuses)
	}
	publish("serenity", true, Contribution{ScaleSet: "serenity-only", RunnerGroup: "Default", MaxRunners: 1, Healthy: true})
	removedAssignment, removedErr := store.AssignOldest(ctx, "serenity-only", now, liveFor)
	if !errors.Is(removedErr, ErrNoAssignment) || removedAssignment != nil {
		t.Fatalf("fully withdrawn scale set retained old demand: %#v, %v", removedAssignment, removedErr)
	}
	publish("serenity", true)

	publish("spock", true, Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 6, Healthy: true})
	if findScaleSet(t, mustScaleSets(t, store, now, liveFor), "meza-ubuntu-slim").EffectiveCapacity != 6 {
		t.Fatal("configuration reload did not replace the host-local limit")
	}
	if findScaleSet(t, mustScaleSets(t, store, now.Add(2*time.Minute), liveFor), "meza-ubuntu-slim").EffectiveCapacity != 0 {
		t.Fatal("expired node remained in effective capacity")
	}
}

func TestPostgreSQLLeaderLeaseFailsOver(t *testing.T) { //nolint:gocyclo // One failover scenario verifies leases, demand continuity, and adapter failure behavior.
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := store.Publish(ctx, "spock", true, []Contribution{
		{ScaleSet: "shared", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
		{ScaleSet: "remote-only", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
	}, now); err != nil {
		t.Fatal(err)
	}
	for requestIndex, scaleSet := range []string{"shared", "remote-only"} {
		if err := store.Enqueue(ctx, Demand{
			ScaleSet: scaleSet, Repository: "repo", RequestID: int64(requestIndex + 1), QueueTime: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.TryLeader(ctx)
	if err != nil || first == nil {
		t.Fatalf("first leader lease = %#v, %v", first, err)
	}
	second, err := store.TryLeader(ctx)
	if !errors.Is(err, ErrNotLeader) || second != nil {
		t.Fatalf("second leader lease = %#v, %v", second, err)
	}
	waiting := make(chan Lease, 1)
	waitingErrors := make(chan error, 1)
	go func() {
		lease, waitErr := store.WaitForLeader(ctx)
		if waitErr != nil {
			waitingErrors <- waitErr
			return
		}
		waiting <- lease
	}()
	if closeErr := first.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
	select {
	case second = <-waiting:
	case waitErr := <-waitingErrors:
		t.Fatal(waitErr)
	case <-time.After(2 * time.Second):
		t.Fatal("blocking leader contender did not acquire the released lease")
	}
	if closeErr := second.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
	for requestIndex, scaleSet := range []string{"shared", "remote-only"} {
		assignment, assignErr := store.AssignOldest(ctx, scaleSet, now, time.Minute)
		if assignErr != nil || assignment == nil || assignment.RequestID != int64(requestIndex+1) {
			t.Fatalf("failover lost %q demand: %#v, %v", scaleSet, assignment, assignErr)
		}
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = (postgreSQLConnection{Conn: connection}).Begin(ctx); err == nil {
		t.Fatal("expected a closed PostgreSQL connection to reject transactions")
	}
	if _, err = (postgreSQLConnection{Conn: connection}).Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("expected a closed PostgreSQL connection to reject queries")
	}
}

func TestPostgreSQLMigrationLifecycleIsReversible(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	now := time.Now()
	assertMigrationStatus(t, store, MigrationStatus{Current: 1, Target: 1, CompatibilityFloor: 1})
	var err error
	if err = store.Publish(ctx, "live-node", true, nil, now); err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateDown(ctx, 0, now, time.Minute); err == nil {
		t.Fatal("expected rollback to reject a live node")
	}
	if err = store.Publish(ctx, "live-node", false, nil, now.Add(-2*time.Minute)); err != nil {
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

func TestPostgreSQLConnectionsUseStableRoleAcrossRotatingLogins(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := admin.Close(ctx); closeErr != nil {
			t.Errorf("close PostgreSQL admin connection: %v", closeErr)
		}
	})
	if _, err = admin.Exec(ctx, `DROP TABLE scaler_schema_version`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE ROLE github_scaler NOLOGIN`,
		`CREATE ROLE wrong_scaler_role NOLOGIN`,
		`CREATE ROLE scaler_one LOGIN PASSWORD 'password-one'`,
		`CREATE ROLE scaler_two LOGIN PASSWORD 'password-two'`,
		`CREATE ROLE scaler_outsider LOGIN PASSWORD 'outsider-password'`,
		`GRANT github_scaler TO scaler_one, scaler_two`,
		`GRANT wrong_scaler_role TO scaler_one, scaler_two`,
		`GRANT USAGE, CREATE ON SCHEMA public TO github_scaler`,
	} {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	credentialDirectory := t.TempDir()
	userPath := filepath.Join(credentialDirectory, "user")
	passwordPath := filepath.Join(credentialDirectory, "password")
	rolePath := filepath.Join(credentialDirectory, "role")
	writeCredential := func(path string, value string) {
		t.Helper()
		if writeErr := os.WriteFile(path, []byte(value+"\n"), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	writeCredential(userPath, "scaler_one")
	writeCredential(passwordPath, "password-one")
	writeCredential(rolePath, "github_scaler")
	t.Setenv("PGOPTIONS", "-c role=wrong_scaler_role")
	settings := ConnectionSettings{
		Host: "localhost", Port: parsedDSN.Port(), Name: "postgres",
		User: credentialFileURL(userPath), Password: credentialFileURL(passwordPath), Role: credentialFileURL(rolePath),
		Direct: &ConnectionEndpoint{Host: "localhost"},
	}
	dynamicStore := NewStore(settings)
	if err = dynamicStore.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	assertPrimaryPostgreSQLSessionRole(t, settings, "scaler_one")
	assertDirectPostgreSQLSessionRole(t, settings, "scaler_one")
	assertStableRoleOwnership(t, admin)
	writeCredential(userPath, "scaler_outsider")
	writeCredential(passwordPath, "outsider-password")
	assertMissingRoleMembershipIsRedacted(t, dynamicStore, userPath, passwordPath, rolePath)
	writeCredential(userPath, "scaler_two")
	writeCredential(passwordPath, "password-two")
	assertPrimaryPostgreSQLSessionRole(t, settings, "scaler_two")
}

func assertStableRoleOwnership(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	for _, table := range []string{"scaler_nodes", "scaler_schema_compatibility", "scaler_schema_version"} {
		var owner string
		if err := admin.QueryRow(context.Background(), `SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`, table).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if owner != "github_scaler" {
			t.Fatalf("%s owner = %q", table, owner)
		}
	}
}

func assertMissingRoleMembershipIsRedacted(t *testing.T, store *Store, referencePaths ...string) {
	t.Helper()
	_, err := store.MigrationStatus(context.Background())
	if err == nil {
		t.Fatal("expected login without stable-role membership to fail")
	}
	for _, secret := range append([]string{"scaler_outsider", "outsider-password", "github_scaler"}, referencePaths...) {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("membership error exposed PostgreSQL configuration: %q", err)
		}
	}
}

func TestPostgreSQLConcurrentStartupMigratesOnceAndElectsOneLeader(t *testing.T) {
	firstStore, _ := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := firstStore.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	stores := []*Store{firstStore, NewStore(firstStore.settings)}
	type startupResult struct {
		lease Lease
		err   error
	}
	results := make(chan startupResult, len(stores))
	for _, store := range stores {
		go func(candidate *Store) {
			if err := candidate.Migrate(ctx); err != nil {
				results <- startupResult{err: err}
				return
			}
			lease, err := candidate.TryLeader(ctx)
			results <- startupResult{lease: lease, err: err}
		}(store)
	}
	leaders := make([]Lease, 0, 1)
	for range stores {
		result := <-results
		if result.err != nil && !errors.Is(result.err, ErrNotLeader) {
			t.Fatal(result.err)
		}
		if result.lease != nil {
			leaders = append(leaders, result.lease)
		}
	}
	if len(leaders) != 1 {
		t.Fatalf("leaders after simultaneous startup = %d", len(leaders))
	}
	if err := leaders[0].Close(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := firstStore.MigrationStatus(ctx)
	if err != nil || status.Current != 1 {
		t.Fatalf("migration status after simultaneous startup = %#v, %v", status, err)
	}
}

func TestPostgreSQLStartupNeverDowngradesANewerCompatibleSchema(t *testing.T) {
	newerStore, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	newerStore.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;")},
		"migrations/002_future.sql": {Data: []byte(`
CREATE TABLE future_schema_marker (value integer NOT NULL);
UPDATE scaler_schema_compatibility SET oldest_binary_schema_generation = 1 WHERE singleton;
---- create above / drop below ----
DROP TABLE future_schema_marker;
`)},
	}
	if err := newerStore.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	olderStore := NewStore(newerStore.settings)
	olderStore.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;")},
	}
	if err := olderStore.Migrate(ctx); err != nil {
		t.Fatalf("compatible previous binary rejected expanded schema: %v", err)
	}
	assertPostgreSQLTableExists(t, dsn, "future_schema_marker")
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(ctx, `UPDATE scaler_schema_compatibility SET oldest_binary_schema_generation = 2 WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = olderStore.Migrate(ctx); err == nil {
		t.Fatal("incompatible previous binary accepted a newer schema")
	}
	assertPostgreSQLTableExists(t, dsn, "future_schema_marker")
}

func TestPostgreSQLFailedMigrationsLeaveTheLastCompleteVersion(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(ctx, `DROP TABLE scaler_schema_version`); err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	initial := []byte(`
CREATE TABLE scaler_schema_compatibility (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    oldest_binary_schema_generation integer NOT NULL
);
INSERT INTO scaler_schema_compatibility (oldest_binary_schema_generation) VALUES (1);
CREATE TABLE scaler_nodes (node_id text PRIMARY KEY, heartbeat_at timestamptz NOT NULL);
CREATE TABLE stable_migration_marker (value integer NOT NULL);
---- create above / drop below ----
DROP TABLE stable_migration_marker;
DROP TABLE scaler_nodes;
DROP TABLE scaler_schema_compatibility;
`)
	store.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: initial},
		"migrations/002_failure.sql": {Data: []byte(`
CREATE TABLE partial_migration_marker (value integer NOT NULL);
SELECT missing_up_migration_function();
---- create above / drop below ----
DROP TABLE partial_migration_marker;
`)},
	}
	if err = store.Migrate(ctx); err == nil {
		t.Fatal("expected failing Up migration")
	}
	status, err := store.MigrationStatus(ctx)
	if err != nil || status.Current != 1 {
		t.Fatalf("status after failed Up = %#v, %v", status, err)
	}
	assertPostgreSQLTableExists(t, dsn, "stable_migration_marker")
	assertPostgreSQLTableMissing(t, dsn, "partial_migration_marker")

	store.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: initial},
		"migrations/002_failure.sql": {Data: []byte(`
CREATE TABLE down_failure_marker (value integer NOT NULL);
---- create above / drop below ----
DROP TABLE down_failure_marker;
SELECT missing_down_migration_function();
`)},
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateDown(ctx, 1, time.Now(), time.Minute); err == nil {
		t.Fatal("expected failing Down migration")
	}
	status, err = store.MigrationStatus(ctx)
	if err != nil || status.Current != 2 {
		t.Fatalf("status after failed Down = %#v, %v", status, err)
	}
	assertPostgreSQLTableExists(t, dsn, "down_failure_marker")
}

func TestPostgreSQLMigrationOrchestrationReportsInvalidDatabaseStates(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	assertMigratorInitializationFailures(t, store)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgreSQLConnection(ctx, admin) })
	assertInvalidVersionMetadata(t, store, admin)
	assertUnavailableSchemaObjects(t, store, dsn, admin)
}

func assertMigratorInitializationFailures(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	store.migrationFiles = failingSubFS{}
	if err := store.Migrate(ctx); err == nil {
		t.Fatal("expected startup migrator initialization failure")
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status migrator initialization failure")
	}
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err == nil {
		t.Fatal("expected rollback migrator initialization failure")
	}
	store.migrationFiles = embeddedMigrations
}

func assertInvalidVersionMetadata(t *testing.T, store *Store, admin *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_schema_version ALTER COLUMN version TYPE text USING version::text; UPDATE scaler_schema_version SET version = 'invalid'`); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err == nil {
		t.Fatal("expected startup version read failure")
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status version read failure")
	}
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err == nil {
		t.Fatal("expected rollback version read failure")
	}
	if _, err := admin.Exec(ctx, `UPDATE scaler_schema_version SET version = '1'; ALTER TABLE scaler_schema_version ALTER COLUMN version TYPE integer USING version::integer`); err != nil {
		t.Fatal(err)
	}
}

func assertUnavailableSchemaObjects(t *testing.T, store *Store, dsn string, admin *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 1, time.Now(), time.Minute); err != nil {
		t.Fatalf("same-version rollback should be a no-op: %v", err)
	}
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_schema_compatibility RENAME TO unavailable_schema_compatibility`); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err == nil {
		t.Fatal("expected startup compatibility read failure")
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status compatibility read failure")
	}
	if _, err := admin.Exec(ctx, `ALTER TABLE unavailable_schema_compatibility RENAME TO scaler_schema_compatibility`); err != nil {
		t.Fatal(err)
	}

	closed, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = closed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.lockedMigrator(ctx, closed); err == nil {
		t.Fatal("expected closed connection to reject migration lock")
	}
	if err = store.ensureRollbackAllowed(ctx, closed, time.Now()); err == nil {
		t.Fatal("expected closed connection to reject leader check")
	}

	if _, err = admin.Exec(ctx, `ALTER TABLE scaler_nodes RENAME TO unavailable_scaler_nodes`); err != nil {
		t.Fatal(err)
	}
	rollbackConnection, err := store.connectDirectPostgreSQL(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureRollbackAllowed(ctx, rollbackConnection, time.Now()); err == nil {
		t.Fatal("expected missing node table to reject live-node check")
	}
	closePostgreSQLConnection(ctx, rollbackConnection)
	if _, err = admin.Exec(ctx, `ALTER TABLE unavailable_scaler_nodes RENAME TO scaler_nodes`); err != nil {
		t.Fatal(err)
	}
}

func assertMigrationStatus(t *testing.T, store *Store, expected MigrationStatus) {
	t.Helper()
	status, err := store.MigrationStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != expected {
		t.Fatalf("migration status = %#v, want %#v", status, expected)
	}
}

func assertPostgreSQLTableExists(t *testing.T, dsn string, table string) {
	t.Helper()
	if !postgreSQLTableExists(t, dsn, table) {
		t.Fatalf("table %q does not exist", table)
	}
}

func assertPostgreSQLTableMissing(t *testing.T, dsn string, table string) {
	t.Helper()
	if postgreSQLTableExists(t, dsn, table) {
		t.Fatalf("table %q still exists", table)
	}
}

func postgreSQLTableExists(t *testing.T, dsn string, table string) bool {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			t.Errorf("close PostgreSQL assertion connection: %v", closeErr)
		}
	}()
	var exists bool
	if err = connection.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func assertPrimaryPostgreSQLSessionRole(t *testing.T, settings ConnectionSettings, expectedSessionUser string) {
	t.Helper()
	connectionConfig, err := settings.config()
	if err != nil {
		t.Fatal(err)
	}
	assertPostgreSQLSessionRole(t, connectionConfig, expectedSessionUser)
}

func assertDirectPostgreSQLSessionRole(t *testing.T, settings ConnectionSettings, expectedSessionUser string) {
	t.Helper()
	connectionConfig, err := settings.directConfig()
	if err != nil {
		t.Fatal(err)
	}
	assertPostgreSQLSessionRole(t, connectionConfig, expectedSessionUser)
}

func assertPostgreSQLSessionRole(t *testing.T, connectionConfig *pgx.ConnConfig, expectedSessionUser string) {
	t.Helper()
	connection, err := pgx.ConnectConfig(context.Background(), connectionConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			t.Errorf("close PostgreSQL role assertion connection: %v", closeErr)
		}
	}()
	var sessionUser string
	var currentUser string
	if err = connection.QueryRow(context.Background(), `SELECT session_user, current_user`).Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != expectedSessionUser || currentUser != "github_scaler" {
		t.Fatalf("session_user=%q current_user=%q", sessionUser, currentUser)
	}
}

func assertPostgreSQLDomainSchemaExists(t *testing.T, dsn string) {
	t.Helper()
	for _, table := range postgreSQLDomainTables() {
		assertPostgreSQLTableExists(t, dsn, table)
	}
}

func assertPostgreSQLDomainSchemaMissing(t *testing.T, dsn string) {
	t.Helper()
	for _, table := range postgreSQLDomainTables() {
		assertPostgreSQLTableMissing(t, dsn, table)
	}
}

func postgreSQLDomainTables() []string {
	return []string{"scaler_nodes", "scaler_contributions", "scaler_demand", "scaler_assignments", "scaler_schema_compatibility"}
}

func postgreSQLTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.Port < 0 {
		t.Fatalf("unexpected PostgreSQL test address: %v", listener.Addr())
	}
	port := uint32(tcpAddress.Port) //nolint:gosec // TCP ports are constrained to the uint16 range.
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	runtimePath := filepath.Join(t.TempDir(), "postgres")
	database := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Port(port).
		RuntimePath(runtimePath).
		Logger(io.Discard))
	if err := database.Start(); err != nil {
		t.Fatalf("start embedded PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Stop(); err != nil {
			t.Errorf("stop embedded PostgreSQL: %v", err)
		}
	})
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	store := NewStore(ConnectionSettings{DSN: dsn, Role: "postgres"})
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store, dsn
}

func assertScaleSetStatus(t *testing.T, store *Store, now time.Time, liveFor time.Duration, expected map[string]ScaleSetStatus) {
	t.Helper()
	statuses := mustScaleSets(t, store, now, liveFor)
	if len(statuses) != len(expected) {
		t.Fatalf("statuses = %#v, want %#v", statuses, expected)
	}
	for _, status := range statuses {
		if status != expected[status.Name] {
			t.Fatalf("status %q = %#v, want %#v", status.Name, status, expected[status.Name])
		}
	}
}

func mustScaleSets(t *testing.T, store *Store, now time.Time, liveFor time.Duration) []ScaleSetStatus {
	t.Helper()
	statuses, err := store.ScaleSets(context.Background(), now, liveFor)
	if err != nil {
		t.Fatal(err)
	}
	return statuses
}

func findScaleSet(t *testing.T, statuses []ScaleSetStatus, name string) ScaleSetStatus {
	t.Helper()
	for _, status := range statuses {
		if status.Name == name {
			return status
		}
	}
	t.Fatalf("scale set %q not found in %#v", name, statuses)
	return ScaleSetStatus{}
}

func hasScaleSet(statuses []ScaleSetStatus, name string) bool {
	for _, status := range statuses {
		if status.Name == name {
			return true
		}
	}
	return false
}
