package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLiveMembershipDoesNotReturnAnUncertainSnapshot(t *testing.T) {
	t.Parallel()
	failure := errors.New("membership database failure")
	for name, transaction := range map[string]transactionStub{
		"query": {query: func(context.Context, string, ...any) (rowsStub, error) { return rowsStub{}, failure }},
		"scan": {query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(failure, nil, nil), nil
		}},
		"iteration": {query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, failure, nil), nil
		}},
		"close": {query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, nil, failure), nil
		}},
		"commit": {query: func(context.Context, string, ...any) (rowsStub, error) { return rowsFromValues(nil), nil }, commit: failingCompletion(failure)},
	} {
		t.Run(name, func(t *testing.T) {
			transaction.rollback = successfulCompletion
			members, err := newStore(executorStub{begin: transactionBeginning(transaction)}).LiveMemberIDs(context.Background(), time.Unix(100, 0))
			if !errors.Is(err, failure) || members != nil {
				t.Fatalf("uncertain membership = %v, %v", members, err)
			}
		})
	}
	members, err := newStore(executorStub{begin: failingBegin(failure)}).LiveMemberIDs(context.Background(), time.Unix(100, 0))
	if !errors.Is(err, failure) || members != nil {
		t.Fatalf("unavailable membership = %v, %v", members, err)
	}
}
