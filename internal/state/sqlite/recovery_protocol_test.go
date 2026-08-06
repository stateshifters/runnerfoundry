package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestClaimOldestRequestsClaimEvidenceInOneTransaction(t *testing.T) {
	statements := make([]string, 0, 3)
	arguments := make([][]any, 0, 3)
	commits := 0
	transaction := Transaction{
		queryRow: func(_ context.Context, statement string, args ...any) Row {
			statements = append(statements, statement)
			arguments = append(arguments, args)
			return rowFromValues(queuedJobValues())
		},
		execute: func(_ context.Context, statement string, args ...any) (Result, error) {
			statements = append(statements, statement)
			arguments = append(arguments, args)
			return successfulResult(), nil
		},
		commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion,
	}
	claim := core.ProcessingClaim{MemberID: testMemberID, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", claim)
	if err != nil || !result.Taken || result.Processing.InventoryEndpointID != claim.InventoryEndpointID || commits != 1 ||
		!strings.Contains(statements[1], "claimed_at, inventory_endpoint_id") || len(arguments[1]) != 7 {
		t.Fatalf("claim = (%#v, %v), statements=%#v args=%#v", result, err, statements, arguments)
	}
}

func TestClaimOldestCommitsEmptySelection(t *testing.T) {
	commits := 0
	transaction := Transaction{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return sql.ErrNoRows }}
	}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{})
	if err != nil || result.Taken || commits != 1 {
		t.Fatalf("empty claim = (%#v, %v), commits=%d", result, err, commits)
	}
}

func TestProcessingByPoolReadsRecoveryEvidence(t *testing.T) {
	values := []any{testJobIDText, "github", "request-3", "linux", string(testMemberID),
		fixedTime().UnixNano(), "docker:member-a", false, false}
	transaction := Transaction{query: func(context.Context, string, ...any) (Rows, error) {
		return rowsFromValues([][]any{values}), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	processing, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].ClaimedAt != fixedTime() ||
		processing[0].InventoryEndpointID != "docker:member-a" {
		t.Fatalf("processing = (%#v, %v)", processing, err)
	}
}

func TestRecoverUnstartedProcessingRequestsGuardedTransition(t *testing.T) {
	statements := make([]string, 0, 2)
	transaction := Transaction{execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
		statements = append(statements, statement)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID, RunnerPool: "linux", EnqueuedAt: fixedTime().Add(time.Minute)},
		ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil || !result.Applied || len(statements) != 2 ||
		!strings.Contains(statements[0], "claimed_at = ?") || !strings.Contains(statements[0], "runner_started = 0") ||
		!strings.Contains(statements[0], "source_tombstones") {
		t.Fatalf("recovery = (%#v, %v), statements=%#v", result, err, statements)
	}
}

func TestNamedQueueAndTerminalResults(t *testing.T) {
	rows := []Row{rowFromValues(queuedJobValues())}
	transaction := Transaction{execute: successfulExecute, queryRow: func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}, commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction)})
	queued, err := store.Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request-3"}, RunnerPool: "linux", EnqueuedAt: fixedTime(),
	})
	if err != nil || !queued.Enqueued || queued.Job.ID != testJobID {
		t.Fatalf("queue = (%#v, %v)", queued, err)
	}
	if removed, err := store.RemoveTerminal(context.Background(), testJobID); err != nil || !removed.Applied {
		t.Fatalf("remove = (%#v, %v)", removed, err)
	}
}

func TestReplaceRegistrationsRequestsOneCommittedSnapshot(t *testing.T) {
	statements := make([]string, 0, 2)
	transaction := Transaction{execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
		statements = append(statements, statement)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	records := []githubsource.Registration{{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}}
	if err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReplaceRegistrations(context.Background(), records); err != nil {
		t.Fatalf("replace registrations: %v", err)
	}
	if len(statements) != 2 || !strings.HasPrefix(statements[0], "DELETE FROM") {
		t.Fatalf("replacement statements = %#v", statements)
	}
}

func TestClaimOldestReportsEveryTransactionFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	claim := core.ProcessingClaim{MemberID: testMemberID, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	if _, err := NewStore(Executor{begin: failingBegin(wantErr)}).ClaimOldest(context.Background(), "linux", claim); err == nil {
		t.Fatal("claim begin failure was ignored")
	}
	for _, transaction := range []Transaction{
		{queryRow: constantRow(Row{scan: func(...any) error { return wantErr }}), rollback: successfulCompletion},
		{queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }}), commit: failingCompletion(wantErr), rollback: successfulCompletion},
		{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: failingExecute(wantErr), rollback: successfulCompletion},
		{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: transactionFailingExecute(2, wantErr).execute, rollback: successfulCompletion},
		{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: successfulExecute,
			commit: failingCompletion(wantErr), rollback: successfulCompletion},
	} {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			ClaimOldest(context.Background(), "linux", claim); err == nil {
			t.Fatal("claim transaction failure was ignored")
		}
	}
}

func TestProcessingByPoolReportsEveryTransactionFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(wantErr)}).ProcessingByPool(context.Background(), "linux"); err == nil {
		t.Fatal("pool lookup begin failure was ignored")
	}
	for index, transaction := range failingListTransactions(wantErr) {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			ProcessingByPool(context.Background(), "linux"); err == nil {
			t.Fatalf("pool lookup failure %d was ignored", index)
		}
	}
}

func TestRecoverUnstartedReportsEveryTransactionFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID, RunnerPool: "linux", EnqueuedAt: fixedTime()},
		ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	if _, err := NewStore(Executor{begin: failingBegin(wantErr)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil {
		t.Fatal("recovery begin failure was ignored")
	}
	rowsAffectedErr := Result{rowsAffected: func() (int64, error) { return 0, wantErr }}
	for _, transaction := range []Transaction{
		{execute: failingExecute(wantErr), rollback: successfulCompletion},
		{execute: func(context.Context, string, ...any) (Result, error) { return rowsAffectedErr, nil }, rollback: successfulCompletion},
		{execute: transactionFailingExecute(2, wantErr).execute, rollback: successfulCompletion},
		{execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion},
	} {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RecoverUnstartedProcessing(context.Background(), processing); err == nil {
			t.Fatal("recovery transaction failure was ignored")
		}
	}
}

func TestReplaceRegistrationsReportsTransactionFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	records := []githubsource.Registration{{RepositoryURL: "repo", RunnerPool: "linux"}}
	executors := []Executor{
		{begin: failingBegin(wantErr)},
		{begin: transactionBeginning(transactionFailingExecute(1, wantErr))},
		{begin: transactionBeginning(transactionFailingExecute(2, wantErr))},
		{begin: transactionBeginning(Transaction{execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion})},
	}
	for index, executor := range executors {
		if err := NewStore(executor).ReplaceRegistrations(context.Background(), records); err == nil {
			t.Fatalf("failure %d was ignored", index)
		}
	}
}
