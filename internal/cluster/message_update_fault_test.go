package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterMessageUpdateRejectsInvalidInputBeforeConnecting(t *testing.T) {
	store := NewStore(Settings{})
	store.connector = connectorThatMustNotRun(t)

	_, err := store.ApplyMessageUpdate(context.Background(), scaler.MessageUpdate{})
	if !errors.Is(err, scaler.ErrInvalidMessageUpdate) {
		t.Fatalf("invalid message update error = %v", err)
	}
}

func TestClusterMessageUpdateReportsConnectionFailure(t *testing.T) {
	failure := errors.New("connect failed")
	store := NewStore(Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, failure }

	if _, err := store.ApplyMessageUpdate(context.Background(), validClusterMessageUpdate()); !errors.Is(err, failure) {
		t.Fatalf("connection error = %v, want %v", err, failure)
	}
}

func TestClusterMessageUpdateReportsBeginFailureAndClosesConnection(t *testing.T) {
	failure := errors.New("begin failed")
	connection := &fakeDatabaseConnection{beginErr: failure}

	_, err := storeWithConnection(connection).ApplyMessageUpdate(context.Background(), validClusterMessageUpdate())
	if !errors.Is(err, failure) || !connection.closed {
		t.Fatalf("begin error = %v, connection closed = %t", err, connection.closed)
	}
}

func TestClusterMessageUpdateReportsEpochQueryFailure(t *testing.T) {
	failure := errors.New("query failed")
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{err: failure}}}

	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), validClusterMessageUpdate(),
	)
	if !errors.Is(err, failure) || transaction.rollbackCalls != 1 {
		t.Fatalf("epoch error = %v, rollbacks = %d", err, transaction.rollbackCalls)
	}
}

func TestClusterMessageUpdateTreatsMissingEpochAsFenced(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}

	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), validClusterMessageUpdate(),
	)
	if !errors.Is(err, scaler.ErrNotLeader) {
		t.Fatalf("missing epoch error = %v, want %v", err, scaler.ErrNotLeader)
	}
}

func TestClusterMessageUpdateReportsCommitFailure(t *testing.T) {
	failure := errors.New("commit failed")
	transaction := successfulClusterMessageTransaction(t)
	transaction.commitErr = failure

	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), validClusterMessageUpdate(),
	)
	if !errors.Is(err, failure) || transaction.rollbackCalls != 0 {
		t.Fatalf("commit error = %v, rollbacks = %d", err, transaction.rollbackCalls)
	}
}

func TestClusterMessageUpdateReportsConnectionCloseFailure(t *testing.T) {
	failure := errors.New("close failed")
	connection := &fakeDatabaseConnection{transaction: successfulClusterMessageTransaction(t), closeErr: failure}

	_, err := storeWithConnection(connection).ApplyMessageUpdate(context.Background(), validClusterMessageUpdate())
	if !errors.Is(err, failure) || !connection.closed {
		t.Fatalf("close error = %v, connection closed = %t", err, connection.closed)
	}
}

func TestClusterMessageUpdateReportsRollbackFailureWithPrimaryError(t *testing.T) {
	queryFailure := errors.New("query failed")
	rollbackFailure := errors.New("rollback failed")
	transaction := &fakeDatabaseTransaction{
		rowResults: []fakeRow{{err: queryFailure}}, rollbackErr: rollbackFailure,
	}

	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), validClusterMessageUpdate(),
	)
	if !errors.Is(err, queryFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("joined rollback error = %v", err)
	}
}

func TestClusterMessageUpdateRollsBackLibvirtIdentityFailure(t *testing.T) {
	failure := errors.New("libvirt update failed")
	transaction := &fakeDatabaseTransaction{
		execErrors: []error{nil, failure}, rowResults: []fakeRow{clusterEpochRow(t, 3)},
	}

	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), clusterMessageUpdateWithStart(),
	)
	failedStatement := transaction.executions[len(transaction.executions)-1].statement
	if !errors.Is(err, failure) || !strings.Contains(failedStatement, "scaler_libvirt_runner_lifecycle") {
		t.Fatalf("libvirt identity error = %v, statement = %q", err, failedStatement)
	}
}

func successfulClusterMessageTransaction(t *testing.T) *fakeDatabaseTransaction {
	t.Helper()
	return &fakeDatabaseTransaction{rowResults: []fakeRow{
		clusterEpochRow(t, 3), availableAssignedJobCapacityRow(t), queuedObligationCountRow(t, 0),
	}}
}
