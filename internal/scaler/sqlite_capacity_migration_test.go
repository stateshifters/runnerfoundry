package scaler

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSQLiteMigrationSixPreservesExactPositiveDemandFIFO(t *testing.T) {
	store := openSQLiteVersionFiveStore(t)
	demand, err := store.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(demand) != 2 || demand[0].RequestID != 22 || demand[1].RequestID != 11 ||
		demand[0].QueueTime != time.Unix(1, 0).UTC() || demand[0].observationOrder != 9 ||
		demand[0].Source != CapacityObligationJobAvailable || demand[0].ObligationID != 9 {
		t.Fatalf("migrated demand = %#v", demand)
	}
}

func TestSQLiteRunnerLifecycleResolvesAssignmentIdentityFromDemand(t *testing.T) {
	store, _ := openSQLiteMigrationStore(t, currentStateSchemaVersion)
	queueTime := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	assignmentID := mustCreateSQLiteJobDemand(t, store, queueTime)
	mustRegisterContradictorySQLiteRunner(t, store, assignmentID, queueTime)
	assertAuthoritativeSQLiteRunner(t, store, assignmentID)
}

func mustCreateSQLiteJobDemand(t *testing.T, store *SQLiteDemandStore, queueTime time.Time) int64 {
	t.Helper()
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "repo", RequestID: 42, QueueTime: queueTime,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	obligations, err := store.Ordered(context.Background(), "linux")
	if err != nil || len(obligations) != 1 {
		t.Fatalf("obligations = %#v, %v", obligations, err)
	}
	return obligations[0].ObligationID
}

func mustRegisterContradictorySQLiteRunner(
	t *testing.T,
	store *SQLiteDemandStore,
	assignmentID int64,
	queueTime time.Time,
) {
	t.Helper()
	err := store.RegisterRunner(context.Background(), RunnerLifecycle{
		NodeID: "local", ScaleSet: "contradictory", Repository: "wrong-repository",
		Provider: RunnerProviderDocker, RunnerName: "linux-42", RequestID: 99,
		AssignmentID: assignmentID, CreatedAt: queueTime,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertAuthoritativeSQLiteRunner(t *testing.T, store *SQLiteDemandStore, assignmentID int64) {
	t.Helper()
	runners, err := store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(runners) != 1 || runners[0].ScaleSet != "linux" || runners[0].Repository != "repo" ||
		runners[0].RequestID != 42 || runners[0].AssignmentID != assignmentID {
		t.Fatalf("runner lifecycle = %#v", runners)
	}
}

func TestSQLiteAssignmentNormalizationRefusesInvalidAggregateState(t *testing.T) {
	store, database := openSQLiteMigrationStore(t, 6)
	mustExecuteSQLiteStatements(t, database, `INSERT INTO runner_lifecycle(
		node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
		phase, created_at, waiting_since, updated_at, reap_reason)
		VALUES('local', 'secret-scale-set', 'secret-repository', 'docker', 'secret-runner',
			42, 999, 'provisioning', 1, 0, 1, '')`)

	err := store.migrate(context.Background())
	if err == nil {
		t.Fatal("expected invalid assignment normalization refusal")
	}
	if !strings.Contains(err.Error(), "missing_demand=1") {
		t.Fatalf("normalization error = %q", err)
	}
	assertErrorOmitsSQLitePayloads(t, err)
	assertSQLiteMigrationState(t, database, 6, 1)
}

func TestSQLiteAssignmentNormalizationPreservesRepresentativeSources(t *testing.T) {
	store, database := openSQLiteMigrationStore(t, 6)
	mustExecuteSQLiteStatements(t, database,
		`INSERT INTO assigned_capacity_observations(
			assigned_observation_id, scale_set, repository, controller_epoch, assigned_jobs, observed_at)
		 VALUES(5, 'linux', 'aggregate-repo', 3, 1, 100)`,
		`INSERT INTO demand(demand_id, observation_order, scale_set, repository, request_id, queue_time,
			acquired, source, assigned_observation_id, controller_epoch, attempt, next_attempt_at)
		 VALUES(10, 10, 'linux', 'exact-repo', 42, 100, 1, 'job_available', NULL, 0, 0, 100),
		       (11, 11, 'linux', 'aggregate-repo', NULL, 101, 1, 'assigned_count', 5, 3, 0, 101)`,
		`INSERT INTO runner_lifecycle(
			node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
			installation_id, github_scale_set_id, github_runner_id, phase, created_at,
			waiting_since, updated_at, reap_reason)
		 VALUES('local', 'linux', 'exact-repo', 'docker', 'exact-runner', 42, 10,
			7, 8, 0, 'provisioning', 100, 0, 100, ''),
		       ('local', 'linux', 'aggregate-repo', 'libvirt', 'aggregate-runner', 99, 11,
			7, 8, 9, 'busy', 100, 100, 101, '')`)
	if err := store.migrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	runners, err := store.RunnerLifecycles(context.Background(), "linux", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(runners) != 2 || runners[0].Repository != "aggregate-repo" || runners[0].RequestID != 99 ||
		runners[1].Repository != "exact-repo" || runners[1].RequestID != 42 {
		t.Fatalf("preserved lifecycles = %#v", runners)
	}
	if _, err = database.Exec(`UPDATE runner_lifecycle SET repository = 'contradictory'
		WHERE runner_name = 'exact-runner'`); err == nil {
		t.Fatal("expected authoritative demand scope enforcement")
	}
}

func TestSQLiteAssignmentNormalizationRollsBackSchemaFailure(t *testing.T) {
	store, database := openSQLiteMigrationStore(t, 6)
	mustExecuteSQLiteSchemaFailureFixture(t, database)
	if err := store.migrate(context.Background()); err == nil {
		t.Fatal("expected assignment-source migration failure")
	}
	var version int
	if err := database.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 6 {
		t.Fatalf("rolled-back schema version = %d", version)
	}
	if _, err := database.Exec(`INSERT INTO runner_lifecycle(
		node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
		phase, created_at, waiting_since, updated_at, reap_reason)
		VALUES('local', 'linux', 'repo', 'docker', 'retained', 1, 0,
			'provisioning', 1, 0, 1, '')`); err != nil {
		t.Fatalf("version-6 lifecycle table was not restored: %v", err)
	}
}

func mustExecuteSQLiteSchemaFailureFixture(t *testing.T, database *sql.DB) {
	t.Helper()
	statement, err := os.ReadFile("testdata/008_schema_failure.sql")
	if err != nil {
		t.Fatal(err)
	}
	mustExecuteSQLiteStatements(t, database, string(statement))
}

func TestSQLiteAssignmentNormalizationRollsBackPreflightFailure(t *testing.T) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close SQLite database: %v", closeErr)
		}
	})
	store := &SQLiteDemandStore{database: database}
	if err = store.normalizeAssignmentSource(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "inspect assignment-source aggregates") {
		t.Fatalf("preflight error = %v", err)
	}
}

func openSQLiteMigrationStore(t *testing.T, targetVersion int) (*SQLiteDemandStore, *sql.DB) {
	t.Helper()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close SQLite database: %v", closeErr)
		}
	})
	database.SetMaxOpenConns(1)
	store := &SQLiteDemandStore{database: database}
	if err = store.migrateTo(context.Background(), targetVersion); err != nil {
		t.Fatal(err)
	}
	return store, database
}

func mustExecuteSQLiteStatements(t *testing.T, database *sql.DB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := database.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func assertErrorOmitsSQLitePayloads(t *testing.T, err error) {
	t.Helper()
	for _, payload := range []string{"secret-scale-set", "secret-repository", "secret-runner"} {
		if strings.Contains(err.Error(), payload) {
			t.Fatalf("normalization error exposed row payload %q: %q", payload, err)
		}
	}
}

func assertSQLiteMigrationState(t *testing.T, database *sql.DB, expectedVersion, expectedRows int) {
	t.Helper()
	var version, retained int
	if err := database.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow("SELECT COUNT(*) FROM runner_lifecycle").Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if version != expectedVersion || retained != expectedRows {
		t.Fatalf("migration state: version=%d retained=%d", version, retained)
	}
}

func openSQLiteVersionFiveStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	store, database := openSQLiteMigrationStore(t, 5)
	_, err := database.Exec(`INSERT INTO demand(observation_order, scale_set, repository, request_id, queue_time, acquired)
		VALUES(9, 'linux', 'repo-b', 22, 1000000000, 0),
		      (10, 'linux', 'repo-a', 11, 2000000000, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}
