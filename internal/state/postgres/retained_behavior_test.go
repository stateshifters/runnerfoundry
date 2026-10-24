package postgres

import (
	"context"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestPublishAndWithdrawMemberCapacity(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	transaction := transactionStub{execute: successfulExecute, commit: successfulCompletion, rollback: successfulCompletion}
	store := newStore(executorStub{begin: transactionBeginning(transaction)})
	if err := store.PublishMemberState(context.Background(), testMemberIDA, now,
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 30}, {RunnerPool: "disabled", Capacity: 0}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := store.WithdrawMemberCapacity(context.Background(), testMemberIDA); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
}

func TestMemberQueries(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	store := newStore(executorStub{queryRow: func(context.Context, string, ...any) rowStub {
		return rowFromValues([]any{1})
	}})
	if count, err := store.CountLiveMembers(context.Background(), now); err != nil || count != 1 {
		t.Fatalf("count = %d, %v", count, err)
	}
}

func TestAvailableCapacity(t *testing.T) {
	t.Parallel()
	store := newStore(executorStub{queryRow: func(context.Context, string, ...any) rowStub {
		return rowFromValues([]any{30, 7, 5})
	}})
	now := fixedTime()
	if capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", now); err != nil || capacity != 18 {
		t.Fatalf("capacity = %d, %v", capacity, err)
	}
}
