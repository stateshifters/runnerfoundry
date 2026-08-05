package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterExpiredReapRetryRepairsMirrorWithoutNewGenericChange(t *testing.T) {
	transaction := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{
		pgconn.NewCommandTag("UPDATE 0"), pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("UPDATE 1"),
	}}
	changed, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RequestExpiredRunnerReaps(
		context.Background(), "linux", "repo", testRunnerLifecycle().CreatedAt, testRunnerLifecycle().CreatedAt,
	)
	if err != nil || changed != 0 {
		t.Fatalf("expired retry = %d, %v", changed, err)
	}
	if !strings.Contains(transaction.executions[1].statement, "pg_advisory_xact_lock") ||
		!strings.Contains(transaction.executions[1].statement, "INSERT INTO scaler_libvirt_runner_lifecycle") {
		t.Fatalf("bulk mirror reconciliation = %s", transaction.executions[1].statement)
	}
}

func TestClusterBulkReapConflictRollsBackGenericMutation(t *testing.T) {
	transaction := &fakeDatabaseTransaction{}
	transaction.queryRowHook = func(statement string, _ ...any) databaseRow {
		if strings.Contains(statement, "cluster_bulk_lifecycle_consistency") {
			return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
				setFakeDestination(destinations[0], true)
			}}}
		}
		return databaseRow{scanner: fakeRow{}}
	}
	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RequestExpiredRunnerReaps(
		context.Background(), "linux", "repo", testRunnerLifecycle().CreatedAt, testRunnerLifecycle().CreatedAt,
	)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("bulk conflict = %v, want lifecycle consistency error", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("bulk conflict rollbacks = %d", transaction.rollbackCalls)
	}
}

func TestClusterBulkMirrorRepairPreservesMultiRowCardinality(t *testing.T) {
	transaction := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{
		pgconn.NewCommandTag("UPDATE 3"), pgconn.NewCommandTag("INSERT 0 3"),
	}}
	changed, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RequestExpiredRunnerReaps(
		context.Background(), "linux", "repo", testRunnerLifecycle().CreatedAt, testRunnerLifecycle().CreatedAt,
	)
	if err != nil || changed != 3 {
		t.Fatalf("multi-row expired reap = %d, %v", changed, err)
	}
	statement := transaction.executions[1].statement
	if !strings.Contains(statement, "SELECT COUNT(*) FROM (SELECT pg_advisory_xact_lock") ||
		!strings.Contains(statement, "FROM lifecycle CROSS JOIN locked") {
		t.Fatalf("bulk lock must produce one barrier row: %s", statement)
	}
}

func TestClusterBulkMirrorRepairUpdatesIdentityFreeRequestExactly(t *testing.T) {
	var validationStatement string
	transaction := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{
		pgconn.NewCommandTag("UPDATE 1"), pgconn.NewCommandTag("INSERT 0 1"),
	}}
	transaction.queryRowHook = func(statement string, _ ...any) databaseRow {
		validationStatement = statement
		return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
			setFakeDestination(destinations[0], false)
		}}}
	}
	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RequestExpiredRunnerReaps(
		context.Background(), "linux", "repo", testRunnerLifecycle().CreatedAt, testRunnerLifecycle().CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if statement := transaction.executions[1].statement; !strings.Contains(statement, "request_id = EXCLUDED.request_id") {
		t.Fatalf("identity-free mirror is not repaired: %s", statement)
	}
	if !strings.Contains(validationStatement, "legacy.request_id <> lifecycle.request_id") {
		t.Fatalf("bulk validation does not require exact request identity: %s", validationStatement)
	}
}
