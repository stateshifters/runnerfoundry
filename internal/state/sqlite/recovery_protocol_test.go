package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestClaimOldestRequestsClaimEvidenceInOneTransaction(t *testing.T) {
	statements := make([]string, 0, 3)
	arguments := make([][]any, 0, 3)
	commits := 0
	transaction := transactionStub{
		queryRow: func(_ context.Context, statement string, args ...any) rowStub {
			statements = append(statements, statement)
			arguments = append(arguments, args)
			return rowFromValues(queuedJobValues())
		},
		execute: func(_ context.Context, statement string, args ...any) (resultStub, error) {
			statements = append(statements, statement)
			arguments = append(arguments, args)
			return successfulResult(), nil
		},
		commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion,
	}
	claim := core.ProcessingClaim{MemberID: testMemberID, AttemptID: core.AttemptID{1}, ClaimedAt: fixedTime(),
		InventoryEndpointID: "docker:member-a"}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", claim)
	if err != nil || result.Outcome != core.ClaimClaimed || result.Processing.InventoryEndpointID != claim.InventoryEndpointID || commits != 1 ||
		!strings.Contains(statements[1], "runner_name, claimed_at, inventory_endpoint_id") || len(arguments[1]) != 8 {
		t.Fatalf("claim = (%#v, %v), statements=%#v args=%#v", result, err, statements, arguments)
	}
}

func TestClaimOldestCommitsEmptySelection(t *testing.T) {
	commits := 0
	transaction := transactionStub{queryRow: func(context.Context, string, ...any) rowStub {
		return rowStub{scan: func(...any) error { return sql.ErrNoRows }}
	}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", validProcessingClaim())
	if err != nil || result.Outcome != core.ClaimEmpty || commits != 1 {
		t.Fatalf("empty claim = (%#v, %v), commits=%d", result, err, commits)
	}
}

func TestClaimOldestPersistsCallerOwnedAttemptIdentity(t *testing.T) {
	transaction := transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub { return rowFromValues(queuedJobValues()) },
		execute:  successfulExecute,
		commit:   successfulCompletion, rollback: successfulCompletion,
	}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ClaimOldest(
		context.Background(), "linux", validProcessingClaim(),
	)
	identity, owned, parseErr := core.ParseRunnerName(result.Processing.RunnerName)
	if err != nil || parseErr != nil || !owned || identity.AttemptID != core.AttemptID([16]byte{1}) {
		t.Fatalf("ClaimOldest() = (%#v, %v), parsed identity = (%#v, %t, %v)",
			result, err, identity, owned, parseErr)
	}
}

func TestProcessingByPoolReadsRecoveryEvidence(t *testing.T) {
	values := []any{testJobIDText, "github", "request-3", "linux", string(testMemberID),
		string(core.AttemptRunnerName(testJobID, core.AttemptID{1})), fixedTime().UnixNano(),
		"docker:member-a", false, false, ""}
	transaction := transactionStub{query: func(context.Context, string, ...any) (rowsStub, error) {
		return rowsFromValues([][]any{values}), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	processing, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].ClaimedAt != fixedTime() ||
		processing[0].InventoryEndpointID != "docker:member-a" {
		t.Fatalf("processing = (%#v, %v)", processing, err)
	}
}

func TestNamedQueueAndTerminalResults(t *testing.T) {
	rows := []rowStub{rowFromValues(queuedJobValues())}
	transaction := transactionStub{execute: successfulExecute, queryRow: func(context.Context, string, ...any) rowStub {
		row := rows[0]
		rows = rows[1:]
		return row
	}, commit: successfulCompletion, rollback: successfulCompletion}
	store := newStore(executorStub{begin: transactionBeginning(transaction)})
	queued, err := store.Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request-3"}, RunnerPool: "linux", EnqueuedAt: fixedTime(),
	})
	if err != nil || queued.Outcome != core.QueueQueued || queued.Job.ID != testJobID {
		t.Fatalf("queue = (%#v, %v)", queued, err)
	}
	if removed, err := store.RemoveTerminal(context.Background(), testJobID); err != nil || removed.Outcome != core.TerminalRemoved {
		t.Fatalf("remove = (%#v, %v)", removed, err)
	}
}

func TestClaimOldestReportsEveryTransactionFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	claim := core.ProcessingClaim{MemberID: testMemberID, AttemptID: core.AttemptID{1}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	if _, err := newStore(executorStub{begin: failingBegin(wantErr)}).ClaimOldest(context.Background(), "linux", claim); err == nil {
		t.Fatal("claim begin failure was ignored")
	}
	for _, transaction := range []transactionStub{
		{queryRow: constantRow(rowStub{scan: func(...any) error { return wantErr }}), rollback: successfulCompletion},
		{queryRow: constantRow(rowStub{scan: func(...any) error { return sql.ErrNoRows }}), commit: failingCompletion(wantErr), rollback: successfulCompletion},
		{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: failingExecute(wantErr), rollback: successfulCompletion},
		{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: transactionFailingExecute(2, wantErr).execute, rollback: successfulCompletion},
		{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: successfulExecute,
			commit: failingCompletion(wantErr), rollback: successfulCompletion},
	} {
		if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
			ClaimOldest(context.Background(), "linux", claim); err == nil {
			t.Fatal("claim transaction failure was ignored")
		}
	}
}

func TestProcessingByPoolReportsEveryTransactionFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	if _, err := newStore(executorStub{begin: failingBegin(wantErr)}).ProcessingByPool(context.Background(), "linux"); err == nil {
		t.Fatal("pool lookup begin failure was ignored")
	}
	for _, testCase := range namedFailingListTransactions(wantErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).
				ProcessingByPool(context.Background(), "linux"); err == nil {
				t.Fatal("pool lookup failure was ignored")
			}
		})
	}
}

func TestRecoverUnstartedReportsEveryTransactionFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID, RunnerPool: "linux", EnqueuedAt: fixedTime()},
		ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	if _, err := newStore(executorStub{begin: failingBegin(wantErr)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil {
		t.Fatal("recovery begin failure was ignored")
	}
	rowsAffectedErr := resultStub{rowsAffected: func() (int64, error) { return 0, wantErr }}
	for _, transaction := range []transactionStub{
		{execute: failingExecute(wantErr), rollback: successfulCompletion},
		{execute: func(context.Context, string, ...any) (resultStub, error) { return rowsAffectedErr, nil }, rollback: successfulCompletion},
		{execute: transactionFailingExecute(2, wantErr).execute, rollback: successfulCompletion},
		{execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion},
	} {
		if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
			RecoverUnstartedProcessing(context.Background(), processing); err == nil {
			t.Fatal("recovery transaction failure was ignored")
		}
	}
}
