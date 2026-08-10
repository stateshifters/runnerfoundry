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
	if err != nil || result.Outcome != core.ClaimClaimed || result.Processing.InventoryEndpointID != claim.InventoryEndpointID || commits != 1 ||
		!strings.Contains(statements[1], "runner_name, claimed_at, inventory_endpoint_id") || len(arguments[1]) != 8 {
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
	if err != nil || result.Outcome != core.ClaimEmpty || commits != 1 {
		t.Fatalf("empty claim = (%#v, %v), commits=%d", result, err, commits)
	}
}

func TestProcessingByPoolReadsRecoveryEvidence(t *testing.T) {
	values := []any{testJobIDText, "github", "request-3", "linux", string(testMemberID),
		string(core.AttemptRunnerName(testJobID, core.JobID{})), fixedTime().UnixNano(),
		"docker:member-a", false, false}
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
	if err != nil || result.Outcome != core.RetryRequeued || len(statements) != 2 ||
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
	if err != nil || queued.Outcome != core.QueueQueued || queued.Job.ID != testJobID {
		t.Fatalf("queue = (%#v, %v)", queued, err)
	}
	if removed, err := store.RemoveTerminal(context.Background(), testJobID); err != nil || removed.Outcome != core.TerminalRemoved {
		t.Fatalf("remove = (%#v, %v)", removed, err)
	}
}

func TestUpsertRegistrationCommitsActiveAndOwnershipTogether(t *testing.T) {
	statements := make([]string, 0, 2)
	arguments := make([][]any, 0, 2)
	commits := 0
	transaction := Transaction{execute: func(_ context.Context, statement string, args ...any) (Result, error) {
		statements = append(statements, statement)
		arguments = append(arguments, args)
		return successfulResult(), nil
	}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	record := githubsource.Registration{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	if err := NewStore(Executor{begin: transactionBeginning(transaction)}).UpsertRegistration(context.Background(), record); err != nil {
		t.Fatalf("upsert registration: %v", err)
	}
	if commits != 1 || len(statements) != 2 ||
		!strings.Contains(statements[0], "runnerfoundry_github_registrations") ||
		!strings.Contains(statements[1], "runnerfoundry_github_scale_set_ownership") ||
		len(arguments[0]) != 5 || len(arguments[1]) != 5 {
		t.Fatalf("upsert protocol = %#v %#v", statements, arguments)
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
	for _, testCase := range namedFailingListTransactions(wantErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
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

func TestUpsertRegistrationRollsBackEveryFailure(t *testing.T) {
	assertRegistrationPairFailures(t, (*Store).UpsertRegistration)
}

func TestDeleteRegistrationStateRollsBackEveryFailure(t *testing.T) {
	assertRegistrationPairFailures(t, (*Store).DeleteRegistrationState)
}

func assertRegistrationPairFailures(
	t *testing.T,
	operation func(*Store, context.Context, githubsource.Registration) error,
) {
	t.Helper()
	wantErr := errors.New("protocol")
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	tests := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: failingBegin(wantErr)}},
		{name: "active", executor: Executor{begin: transactionBeginning(transactionFailingExecute(1, wantErr))}},
		{name: "ownership", executor: Executor{begin: transactionBeginning(transactionFailingExecute(2, wantErr))}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{
			execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion,
		})}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := operation(NewStore(testCase.executor), context.Background(), record); err == nil {
				t.Fatal("failure was ignored")
			}
		})
	}
}

func TestOwnedRegistrationsReturnsDurableOwnership(t *testing.T) {
	transaction := Transaction{query: func(context.Context, string, ...any) (Rows, error) {
		return rowsFromValues([][]any{{"repo", "linux", int64(1), int64(2), int64(3)}}), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	registrations, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		OwnedRegistrations(context.Background())
	if err != nil || len(registrations) != 1 || registrations[0].ScaleSetID != 3 {
		t.Fatalf("owned registrations = (%#v, %v)", registrations, err)
	}
}

func TestOwnedRegistrationsReportsTransactionFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	tests := []namedTransaction{
		{name: "query", transaction: Transaction{query: func(context.Context, string, ...any) (Rows, error) {
			return Rows{}, wantErr
		}, rollback: successfulCompletion}},
		{name: "commit", transaction: Transaction{query: func(context.Context, string, ...any) (Rows, error) {
			return rowsFromValues(nil), nil
		},
			commit: failingCompletion(wantErr), rollback: successfulCompletion}},
	}
	if _, err := NewStore(Executor{begin: failingBegin(wantErr)}).OwnedRegistrations(context.Background()); err == nil {
		t.Fatal("begin failure was ignored")
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				OwnedRegistrations(context.Background()); err == nil {
				t.Fatal("failure was ignored")
			}
		})
	}
}

func TestDeleteRegistrationStateGuardsAndCommitsBothRecords(t *testing.T) {
	statements := make([]string, 0, 2)
	arguments := make([][]any, 0, 2)
	transaction := Transaction{execute: func(_ context.Context, statement string, args ...any) (Result, error) {
		statements = append(statements, statement)
		arguments = append(arguments, args)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	if err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		DeleteRegistrationState(context.Background(), record); err != nil {
		t.Fatalf("delete registration state: %v", err)
	}
	if len(statements) != 2 || !strings.Contains(statements[0], "runnerfoundry_github_registrations") ||
		!strings.Contains(statements[1], "runnerfoundry_github_scale_set_ownership") ||
		len(arguments[0]) != 5 || len(arguments[1]) != 5 {
		t.Fatalf("delete protocol = %#v %#v", statements, arguments)
	}
}
