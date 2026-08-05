package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestEnqueuePersistsAndReturnsRepositoryAssignedJob(t *testing.T) {
	t.Parallel()
	enqueuedAt := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store, commitCalls := successfulEnqueueStore(enqueuedAt)
	record, created, err := store.Enqueue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request-7"}, RunnerPool: "linux", EnqueuedAt: enqueuedAt,
	})
	if err != nil || !created || record.Job.ID != testJobID || record.State != core.JobStateQueued || *commitCalls != 1 {
		t.Fatalf("enqueue = %#v, %t, commits=%d, error=%v", record, created, *commitCalls, err)
	}
}

func TestEnqueueDoesNotRecreateFinalizedSourceRequest(t *testing.T) {
	t.Parallel()
	store, queryCalls := finalizedRedeliveryStore("request-7")
	record, created, err := store.Enqueue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request-7"}, RunnerPool: "linux",
	})
	if err != nil || created || record != (core.JobRecord{}) || *queryCalls != 2 {
		t.Fatalf("redelivery = %#v, %t, queries=%d, error=%v", record, created, *queryCalls, err)
	}
}

func successfulEnqueueStore(enqueuedAt time.Time) (*Store, *int) {
	commitCalls := 0
	transaction := Transaction{
		execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		queryRow: func(context.Context, string, ...any) Row {
			return Row{scan: func(destinations ...any) error {
				return assignDestinations(destinations,
					[]any{testJobIDText, "github", "request-7", "linux", "queued", enqueuedAt.UnixNano()})
			}}
		},
		commit: func(context.Context) error { commitCalls++; return nil }, rollback: func(context.Context) error { return nil },
	}
	return NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }}), &commitCalls
}

func finalizedRedeliveryStore(sourceID string) (*Store, *int) {
	queryCalls := 0
	transaction := Transaction{
		execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
		},
		queryRow: func(context.Context, string, ...any) Row {
			queryCalls++
			if queryCalls == 1 {
				return Row{scan: func(...any) error { return sql.ErrNoRows }}
			}
			return Row{scan: func(destinations ...any) error {
				return assignDestinations(destinations, []any{"github", sourceID, "failure", "service", "reported"})
			}}
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	return NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }}), &queryCalls
}

func TestBeginAcquisitionPersistsFirstMarker(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 5, 12, 5, 0, 0, time.UTC)
	var statement string
	store := NewStore(Executor{
		begin: func(context.Context) (Transaction, error) { return Transaction{}, errors.New("begin not expected") },
		queryRow: func(_ context.Context, query string, _ ...any) Row {
			statement = query
			return Row{scan: func(destinations ...any) error {
				values := []any{testJobIDText, "github", "request-7", "linux", "attempting", at.Add(-time.Minute).UnixNano(),
					sql.NullString{String: testMemberIDText, Valid: true}, sql.NullInt64{Int64: at.Add(-time.Second).UnixNano(), Valid: true},
					sql.NullInt64{}, sql.NullInt64{Int64: at.UnixNano(), Valid: true}}
				return assignDestinations(destinations, values)
			}}
		},
	})
	record, err := store.BeginAcquisition(context.Background(), testJobID, at)
	if err != nil || !record.AcquisitionStartedAt.Equal(at) || !strings.Contains(statement, "COALESCE") {
		t.Fatalf("begin acquisition = %#v, statement=%q, error=%v", record, statement, err)
	}
}

func TestSetResultPendingIsIdempotentForSameResult(t *testing.T) {
	t.Parallel()
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "cleanup"}
	transaction := resultPendingTransaction(sqlitePendingJobValues(result), nil)
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	record, err := store.SetResultPending(context.Background(), testJobID, result)
	if err != nil || record.Result == nil || *record.Result != result {
		t.Fatalf("same result = %#v, %v", record, err)
	}
}

func TestSetResultPendingRejectsDifferentResult(t *testing.T) {
	t.Parallel()
	existing := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "cleanup"}
	transaction := resultPendingTransaction(nil, sqlitePendingJobValues(existing))
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	_, err := store.SetResultPending(context.Background(), testJobID,
		core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner})
	if !errors.Is(err, core.ErrConflictingResult) {
		t.Fatalf("conflicting result error = %v", err)
	}
}

func TestCapacityPublicationCannotMutateJobs(t *testing.T) {
	t.Parallel()
	var statement string
	transaction := Transaction{
		execute: func(_ context.Context, query string, _ ...any) (Result, error) {
			statement = query
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	if err := store.PublishRunnerPoolCapacity(context.Background(), testMemberID, "linux", 2); err != nil {
		t.Fatalf("publish capacity: %v", err)
	}
	if strings.Contains(statement, "runnerfoundry_jobs") || !strings.Contains(statement, "member_runner_pools") {
		t.Fatalf("capacity statement crossed job boundary: %s", statement)
	}
}

func TestFinalizeWritesTombstoneBeforeRemovingJob(t *testing.T) {
	t.Parallel()
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "cleanup"}
	var statements []string
	transaction := Transaction{
		execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
			statements = append(statements, statement)
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		queryRow: func(context.Context, string, ...any) Row {
			return Row{scan: func(destinations ...any) error {
				return assignDestinations(destinations, sqlitePendingJobValues(result))
			}}
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	receipt, err := store.Finalize(context.Background(), testJobID, core.ResultDeliveryReported)
	if err != nil || receipt.Outcome != result.Outcome || len(statements) != 2 ||
		!strings.Contains(statements[0], "source_receipts") || !strings.Contains(statements[1], "DELETE") {
		t.Fatalf("finalize receipt=%#v statements=%#v error=%v", receipt, statements, err)
	}
}

func TestExpireMemberRemovesCapacityBeforeRetainingReferencedIdentity(t *testing.T) {
	t.Parallel()
	var statements []string
	transaction := Transaction{
		execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
			statements = append(statements, statement)
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	if err := store.ExpireMember(context.Background(), testMemberID); err != nil {
		t.Fatalf("expire member: %v", err)
	}
	if len(statements) != 2 || !strings.Contains(statements[0], "member_runner_pools") ||
		!strings.Contains(statements[1], "NOT EXISTS") {
		t.Fatalf("expiry statements = %#v", statements)
	}
}

func TestUnstartedTimeoutIsScopedToRunnerPool(t *testing.T) {
	t.Parallel()
	var statement string
	var arguments []any
	transaction := Transaction{
		query: func(_ context.Context, query string, values ...any) (Rows, error) {
			statement, arguments = query, values
			return emptyRows(), nil
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	_, err := store.SetUnstartedAttemptsResultPending(context.Background(), "linux", time.Unix(0, 2).UTC(),
		core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService})
	if err != nil || !strings.Contains(statement, "runner_pool = ?") || len(arguments) != 5 || arguments[3] != "linux" {
		t.Fatalf("pool timeout statement=%q arguments=%#v error=%v", statement, arguments, err)
	}
}

func emptyRows() Rows {
	return Rows{close: func() error { return nil }, err: func() error { return nil }, next: func() bool { return false },
		scan: func(...any) error { return errors.New("scan not expected") }}
}

func resultPendingTransaction(updateValues, lookupValues []any) Transaction {
	queryCalls := 0
	return Transaction{
		queryRow: func(context.Context, string, ...any) Row {
			queryCalls++
			values := updateValues
			if queryCalls > 1 {
				values = lookupValues
			}
			if values == nil {
				return Row{scan: func(...any) error { return sql.ErrNoRows }}
			}
			return Row{scan: func(destinations ...any) error { return assignDestinations(destinations, values) }}
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
}

func sqlitePendingJobValues(result core.Result) []any {
	return []any{testJobIDText, "github", "request-7", "linux", "result_pending", int64(1), sql.NullString{},
		sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
		sql.NullString{String: string(result.Outcome), Valid: true},
		sql.NullString{String: string(result.Origin), Valid: true}, sql.NullString{String: result.Reason, Valid: true}}
}

func assignDestinations(destinations, values []any) error {
	for index, value := range values {
		destination := reflect.ValueOf(destinations[index])
		source := reflect.ValueOf(value)
		if destination.Kind() != reflect.Pointer || !source.Type().AssignableTo(destination.Elem().Type()) {
			return fmt.Errorf("cannot assign %T to %T", value, destinations[index])
		}
		destination.Elem().Set(source)
	}
	return nil
}
