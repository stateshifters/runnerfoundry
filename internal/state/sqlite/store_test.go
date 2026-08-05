package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
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

	processing, found, err := store.TakeOldest(context.Background(), "linux", testMemberID)

	if err != nil || !found || processing.Job.ID != testJobID || processing.MemberID != testMemberID {
		t.Fatalf("take oldest = %#v, %t, %v", processing, found, err)
	}
	if commits != 1 || len(statements) != 4 {
		t.Fatalf("transaction effects = %#v, commits=%d", statements, commits)
	}
	if !strings.Contains(statements[0], "ORDER BY enqueued_at LIMIT 1") {
		t.Fatalf("selection is not SQL-owned FIFO: %s", statements[0])
	}
}

func TestTakeOldestRollsBackEveryPartialMove(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for failureCall := 1; failureCall <= 3; failureCall++ {
		failureCall := failureCall
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
		_, _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			TakeOldest(context.Background(), "linux", testMemberID)
		if err == nil || rollbacks != 1 {
			t.Fatalf("failure %d: err=%v rollbacks=%d", failureCall, err, rollbacks)
		}
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
	_, found, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		TakeOldest(context.Background(), "linux", testMemberID)
	if err != nil || found {
		t.Fatalf("empty queue = %t, %v", found, err)
	}
}

func TestCancelQueuedAlwaysTombstonesTheSource(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 2)
	transaction := Transaction{execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
		statements = append(statements, statement)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	source := core.SourceReference{Adapter: "github", ID: "request-3"}
	if err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		CancelQueued(context.Background(), source); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	if len(statements) != 2 || !strings.Contains(statements[0], "source_tombstones") ||
		!strings.Contains(statements[1], "queued_jobs") {
		t.Fatalf("cancellation effects = %#v", statements)
	}
}

func queuedJobValues() []any {
	return []any{testJobIDText, "github", "request-3", "linux", fixedTime().UnixNano()}
}

func successfulResult() Result {
	return Result{rowsAffected: func() (int64, error) { return 1, nil }}
}
