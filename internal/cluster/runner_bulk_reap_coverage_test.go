package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestExcessRunnerReapCoversTransactionOutcomes(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1, 0).UTC()
	failure := errors.New("failed")
	tests := map[string]*fakeDatabaseConnection{
		"begin":      {beginErr: failure},
		"update":     {transaction: &fakeDatabaseTransaction{execErrors: []error{failure}}},
		"mirror":     {transaction: &fakeDatabaseTransaction{execErrors: []error{nil, failure}}},
		"validation": {transaction: bulkValidationFailureTransaction(failure)},
		"commit":     {transaction: &fakeDatabaseTransaction{commitErr: failure}},
	}
	for name, connection := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := storeWithConnection(connection).RequestExcessWaitingRunnerReaps(
				ctx, "linux", "repo", 1, "excess", now,
			)
			if err == nil {
				t.Fatal("expected excess reap failure")
			}
		})
	}

	store := &Store{connector: func(context.Context) (*databaseConnection, error) { return nil, failure }}
	if _, err := store.RequestExcessWaitingRunnerReaps(ctx, "linux", "repo", 1, "excess", now); err == nil {
		t.Fatal("expected excess reap connection failure")
	}
}

func bulkValidationFailureTransaction(failure error) *fakeDatabaseTransaction {
	transaction := &fakeDatabaseTransaction{}
	transaction.queryRowHook = func(string, ...any) databaseRow {
		return databaseRow{scanner: fakeRow{err: failure}}
	}
	return transaction
}

func TestExcessRunnerReapClampsNegativeRetention(t *testing.T) {
	transaction := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{
		pgconn.NewCommandTag("UPDATE 2"), pgconn.NewCommandTag("INSERT 0 2"),
	}}
	changed, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RequestExcessWaitingRunnerReaps(
		context.Background(), "linux", "repo", -1, "excess", time.Unix(1, 0).UTC(),
	)
	if err != nil || changed != 2 {
		t.Fatalf("excess reap = %d, %v", changed, err)
	}
	if retain := transaction.executions[0].arguments[2]; retain != 0 {
		t.Fatalf("clamped retention = %v, want 0", retain)
	}
}
