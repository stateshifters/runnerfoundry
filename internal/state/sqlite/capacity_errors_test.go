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
		executor executorStub
	}{
		{name: "begin", executor: executorStub{begin: failingBegin(protocolErr)}},
		{name: "member heartbeat", executor: executorStub{begin: transactionBeginning(transactionStub{execute: failingExecute(protocolErr), rollback: successfulCompletion})}},
		{name: "capacity withdrawal", executor: executorStub{begin: transactionBeginning(transactionFailingExecute(2, protocolErr))}},
		{name: "capacity publication", executor: executorStub{begin: transactionBeginning(transactionFailingExecute(3, protocolErr))}},
		{name: "commit", executor: executorStub{begin: transactionBeginning(transactionStub{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion})}},
		{name: "rollback", executor: executorStub{begin: transactionBeginning(transactionStub{execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr)})}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := newStore(testCase.executor).PublishMemberState(context.Background(), testMemberID, fixedTime(),
				[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}}); err == nil {
				t.Fatal("publish succeeded")
			}
		})
	}
}

func TestWithdrawMemberCapacityProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if err := newStore(executorStub{begin: failingBegin(protocolErr)}).WithdrawMemberCapacity(context.Background(), testMemberID); err == nil {
		t.Fatal("expected withdrawal begin failure")
	}
	for _, testCase := range []struct {
		name        string
		transaction transactionStub
	}{
		{name: "withdrawal", transaction: transactionFailingExecute(1, protocolErr)},
		{name: "commit", transaction: transactionStub{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).WithdrawMemberCapacity(context.Background(), testMemberID); err == nil {
				t.Fatal("withdrawal succeeded")
			}
		})
	}
	if err := newStore(executorStub{begin: transactionBeginning(transactionStub{execute: successfulExecute,
		commit: successfulCompletion, rollback: successfulCompletion})}).WithdrawMemberCapacity(context.Background(), testMemberID); err != nil {
		t.Fatalf("withdraw member capacity: %v", err)
	}
}

func TestNonPositiveCapacityIsWithdrawn(t *testing.T) {
	t.Parallel()
	if err := newStore(executorStub{begin: transactionBeginning(transactionStub{execute: successfulExecute,
		commit: successfulCompletion, rollback: successfulCompletion})}).PublishMemberState(context.Background(), testMemberID,
		fixedTime(), []core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 0}}); err != nil {
		t.Fatalf("withdraw non-positive capacity: %v", err)
	}
}

func TestCapacityScalarProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	brokenScalar := newStore(executorStub{queryRow: constantRow(rowStub{scan: func(...any) error { return protocolErr }})})
	if _, err := brokenScalar.CountLiveMembers(context.Background(), fixedTime()); err == nil {
		t.Fatal("expected count error")
	}
	if _, err := brokenScalar.AvailableRunnerPoolCapacity(context.Background(), "linux", fixedTime()); err == nil {
		t.Fatal("expected capacity error")
	}
}
