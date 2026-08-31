package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestTakeOldestMovesOneQueuedJobToProcessingAtomically(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 4)
	commits := 0
	transaction := Transaction{
		queryRow: func(_ context.Context, statement string, _ ...any) Row {
			statements = append(statements, statement)
			return rowFromValues(queuedJobValues())
		},
		execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
			statements = append(statements, statement)
			return successfulResult(), nil
		},
		commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion,
	}
	store := NewStore(Executor{begin: transactionBeginning(transaction)})

	result, err := store.ClaimOldest(context.Background(), "linux",
		core.ProcessingClaim{MemberID: testMemberID, AttemptID: core.AttemptID{1}})

	if err != nil || result.Outcome != core.ClaimClaimed || result.Processing.Job.ID != testJobID || result.Processing.MemberID != testMemberID {
		t.Fatalf("claim oldest = %#v, %v", result, err)
	}
	if commits != 1 || len(statements) != 3 {
		t.Fatalf("transaction effects = %#v, commits=%d", statements, commits)
	}
	if !strings.Contains(statements[0], "ORDER BY enqueued_at LIMIT 1") {
		t.Fatalf("selection is not SQL-owned FIFO: %s", statements[0])
	}
}

func TestTakeOldestRollsBackEveryPartialMove(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for name, failureCall := range map[string]int{"processing insertion": 1, "queue removal": 2} {
		t.Run(name, func(t *testing.T) {
			assertPartialClaimRollback(t, failureCall, protocolErr)
		})
	}
}

func assertPartialClaimRollback(t *testing.T, failureCall int, protocolErr error) {
	t.Helper()
	executeCalls, rollbacks := 0, 0
	transaction := Transaction{
		queryRow: func(context.Context, string, ...any) Row { return rowFromValues(queuedJobValues()) },
		execute: func(context.Context, string, ...any) (Result, error) {
			executeCalls++
			if executeCalls == failureCall {
				return Result{}, protocolErr
			}
			return successfulResult(), nil
		},
		commit:   successfulCompletion,
		rollback: func(context.Context) error { rollbacks++; return nil },
	}
	_, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberID})
	if err == nil || rollbacks != 1 {
		t.Fatalf("err=%v rollbacks=%d", err, rollbacks)
	}
}

func TestTakeOldestReportsAnEmptyQueue(t *testing.T) {
	t.Parallel()
	transaction := Transaction{
		queryRow: func(context.Context, string, ...any) Row {
			return Row{scan: func(...any) error { return sql.ErrNoRows }}
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberID})
	if err != nil || result.Outcome != core.ClaimEmpty {
		t.Fatalf("empty queue = %#v, %v", result, err)
	}
}

func TestTerminalizeSourceTombstonesAndDeletesQueued(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 2)
	transaction := Transaction{execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
		statements = append(statements, statement)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	source := core.SourceReference{Adapter: "github", ID: "request-3"}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		Terminalize(context.Background(), core.JobIdentity{Source: source}); err != nil {
		t.Fatalf("terminalize source: %v", err)
	}
	if len(statements) != 2 || !strings.Contains(statements[0], "source_tombstones") ||
		!strings.Contains(statements[1], "queued_jobs") {
		t.Fatalf("cancellation effects = %#v", statements)
	}
}

func TestObserveRunnerAcceptanceTombstonesAQueuedRetry(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 3)
	transaction := Transaction{execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
		statements = append(statements, statement)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	_, err := NewStore(Executor{begin: transactionBeginning(transaction)}).ObserveRunnerAcceptance(context.Background(),
		core.JobIdentity{Source: core.SourceReference{Adapter: "github", ID: "request-3"}})
	if err != nil || len(statements) != 3 || !strings.Contains(statements[0], "runner_started") ||
		!strings.Contains(statements[0], "runner_started = 0") ||
		!strings.Contains(statements[1], "source_tombstones") || !strings.Contains(statements[2], "queued_jobs") {
		t.Fatalf("delayed acceptance statements=%#v error=%v", statements, err)
	}
}

func TestRetryUnstartedProcessingMovesSameJobToDatabaseTimedQueueTail(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 2)
	processingValues := claimedProcessingJobValues()
	processingValues[8], processingValues[9] = false, false
	transaction := Transaction{
		queryRow: func(context.Context, string, ...any) Row { return rowFromValues(processingValues) },
		execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
			statements = append(statements, statement)
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob())
	if err != nil || result.Outcome != core.RetryRequeued || len(statements) != 2 || !strings.Contains(statements[0], "julianday('now')") ||
		!strings.Contains(statements[0], "runner_started = 0") {
		t.Fatalf("retry statements=%#v result=%#v error=%v", statements, result, err)
	}
}

func queuedJobValues() []any {
	return []any{testJobIDText, "github", "request-3", "linux", fixedTime().UnixNano()}
}

func successfulResult() Result {
	return Result{rowsAffected: func() (int64, error) { return 1, nil }}
}
