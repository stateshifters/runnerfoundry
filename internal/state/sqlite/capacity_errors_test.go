package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestPublishMemberStateProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for index, executor := range []Executor{
		{begin: failingBegin(protocolErr)},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(transactionFailingExecute(2, protocolErr))},
		{begin: transactionBeginning(transactionFailingExecute(3, protocolErr))},
		{begin: transactionBeginning(Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr)})},
	} {
		if err := NewStore(executor).PublishMemberState(context.Background(), testMemberID, fixedTime(),
			[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}}); err == nil {
			t.Fatalf("publish case %d", index)
		}
	}
}

func TestWithdrawMemberCapacityProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if err := NewStore(Executor{begin: failingBegin(protocolErr)}).WithdrawMemberCapacity(context.Background(), testMemberID); err == nil {
		t.Fatal("expected withdrawal begin failure")
	}
	for index, transaction := range []Transaction{
		transactionFailingExecute(1, protocolErr),
		{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	} {
		if err := NewStore(Executor{begin: transactionBeginning(transaction)}).WithdrawMemberCapacity(context.Background(), testMemberID); err == nil {
			t.Fatalf("withdrawal case %d", index)
		}
	}
	if err := NewStore(Executor{begin: transactionBeginning(Transaction{execute: successfulExecute,
		commit: successfulCompletion, rollback: successfulCompletion})}).WithdrawMemberCapacity(context.Background(), testMemberID); err != nil {
		t.Fatalf("withdraw member capacity: %v", err)
	}
}

func TestNonPositiveCapacityIsWithdrawn(t *testing.T) {
	t.Parallel()
	if err := NewStore(Executor{begin: transactionBeginning(Transaction{execute: successfulExecute,
		commit: successfulCompletion, rollback: successfulCompletion})}).PublishMemberState(context.Background(), testMemberID,
		fixedTime(), []core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 0}}); err != nil {
		t.Fatalf("withdraw non-positive capacity: %v", err)
	}
}

func TestCapacityScalarProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	brokenScalar := NewStore(Executor{queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }})})
	if _, err := brokenScalar.CountLiveMembers(context.Background(), fixedTime()); err == nil {
		t.Fatal("expected count error")
	}
	if _, err := brokenScalar.AvailableRunnerPoolCapacity(context.Background(), "linux", fixedTime()); err == nil {
		t.Fatal("expected capacity error")
	}
}
