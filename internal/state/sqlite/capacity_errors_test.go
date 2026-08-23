package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestPublishMemberStateProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: failingBegin(protocolErr)}},
		{name: "member heartbeat", executor: Executor{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})}},
		{name: "capacity withdrawal", executor: Executor{begin: transactionBeginning(transactionFailingExecute(2, protocolErr))}},
		{name: "capacity publication", executor: Executor{begin: transactionBeginning(transactionFailingExecute(3, protocolErr))}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion})}},
		{name: "rollback", executor: Executor{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr)})}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := NewStore(testCase.executor).PublishMemberState(context.Background(), testMemberID, fixedTime(),
				[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}}); err == nil {
				t.Fatal("publish succeeded")
			}
		})
	}
}

func TestWithdrawMemberCapacityProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if err := NewStore(Executor{begin: failingBegin(protocolErr)}).WithdrawMemberCapacity(context.Background(), testMemberID); err == nil {
		t.Fatal("expected withdrawal begin failure")
	}
	for _, testCase := range []struct {
		name        string
		transaction Transaction
	}{
		{name: "withdrawal", transaction: transactionFailingExecute(1, protocolErr)},
		{name: "commit", transaction: Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).WithdrawMemberCapacity(context.Background(), testMemberID); err == nil {
				t.Fatal("withdrawal succeeded")
			}
		})
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
