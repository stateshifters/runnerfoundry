package postgres

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

func TestClaimOldestSerializesPoolBeforeTimestampOnlyFIFOClaim(t *testing.T) {
	t.Parallel()
	queuedAt := time.Date(2026, time.August, 5, 11, 0, 0, 0, time.UTC)
	claimedAt := queuedAt.Add(time.Minute)
	store, protocol := claimStore(queuedAt, claimedAt)
	record, claimed, err := store.ClaimOldest(context.Background(), "linux", testMemberIDA, claimedAt)
	if err != nil || !claimed || record.Job.ID != testJobID4 {
		t.Fatalf("claim = %#v, %t, %v", record, claimed, err)
	}
	claimStatement := protocol.statements[1]
	if len(protocol.statements) != 2 || !strings.Contains(protocol.statements[0], "pg_advisory_xact_lock") ||
		!strings.Contains(claimStatement, "WHERE runner_pool = $3 AND state = 'queued'") ||
		!strings.Contains(claimStatement, "ORDER BY enqueued_at LIMIT 1") ||
		strings.Contains(claimStatement, "ORDER BY enqueued_at,") || protocol.commitCalls != 1 {
		t.Fatalf("claim protocol = %#v, commits=%d", protocol.statements, protocol.commitCalls)
	}
}

func TestEnqueueDoesNotRecreateFinalizedSourceRequest(t *testing.T) {
	t.Parallel()
	store, queryCalls := finalizedRedeliveryStore("request-4")
	record, created, err := store.Enqueue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request-4"}, RunnerPool: "linux",
	})
	if err != nil || created || record != (core.JobRecord{}) || *queryCalls != 2 {
		t.Fatalf("redelivery = %#v, %t, queries=%d, error=%v", record, created, *queryCalls, err)
	}
}

type claimProtocol struct {
	statements  []string
	commitCalls int
}

func claimStore(queuedAt, claimedAt time.Time) (*Store, *claimProtocol) {
	protocol := &claimProtocol{}
	transaction := Transaction{
		execute: func(_ context.Context, statement string, _ ...any) (Result, error) {
			protocol.statements = append(protocol.statements, statement)
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		queryRow: func(_ context.Context, statement string, _ ...any) Row {
			protocol.statements = append(protocol.statements, statement)
			return Row{scan: func(destinations ...any) error {
				return assignDestinations(destinations, []any{testJobID4, "github", "request-4", "linux", "attempting",
					queuedAt, validNullUUID(testMemberIDA), sql.NullTime{Time: claimedAt, Valid: true}})
			}}
		},
		commit: func(context.Context) error { protocol.commitCalls++; return nil }, rollback: func(context.Context) error { return nil },
	}
	return NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }}), protocol
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
				values := []any{testJobID7, "github", "request-7", "linux", "attempting", at.Add(-time.Minute),
					validNullUUID(testMemberIDA), sql.NullTime{Time: at.Add(-time.Second), Valid: true},
					sql.NullTime{}, sql.NullTime{Time: at, Valid: true}}
				return assignDestinations(destinations, values)
			}}
		},
	})
	record, err := store.BeginAcquisition(context.Background(), testJobID7, at)
	if err != nil || !record.AcquisitionStartedAt.Equal(at) || !strings.Contains(statement, "COALESCE") {
		t.Fatalf("begin acquisition = %#v, statement=%q, error=%v", record, statement, err)
	}
}

func TestSetResultPendingIsIdempotentForSameResult(t *testing.T) {
	t.Parallel()
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "cleanup"}
	transaction := resultPendingTransaction(postgresPendingJobValues(result), nil)
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	record, err := store.SetResultPending(context.Background(), testJobID7, result)
	if err != nil || record.Result == nil || *record.Result != result {
		t.Fatalf("same result = %#v, %v", record, err)
	}
}

func TestSetResultPendingRejectsDifferentResult(t *testing.T) {
	t.Parallel()
	existing := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "cleanup"}
	transaction := resultPendingTransaction(nil, postgresPendingJobValues(existing))
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	_, err := store.SetResultPending(context.Background(), testJobID7,
		core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner})
	if !errors.Is(err, core.ErrConflictingResult) {
		t.Fatalf("conflicting result error = %v", err)
	}
}

func TestMemberStatePublicationReplacesCapacityWithoutMutatingJobs(t *testing.T) {
	t.Parallel()
	var statements []string
	transaction := Transaction{
		execute: func(_ context.Context, query string, _ ...any) (Result, error) {
			statements = append(statements, query)
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	if err := store.PublishMemberState(context.Background(), testMemberIDA, fixedTime(), []core.RunnerPoolCapacity{
		{RunnerPool: "linux", Capacity: 2}, {RunnerPool: "disabled", Capacity: 0},
	}); err != nil {
		t.Fatalf("publish member state: %v", err)
	}
	if len(statements) != 3 || !strings.Contains(statements[0], "runnerfoundry_members") ||
		!strings.Contains(statements[1], "DELETE FROM public.runnerfoundry_member_runner_pools") ||
		!strings.Contains(statements[2], "INSERT INTO public.runnerfoundry_member_runner_pools") {
		t.Fatalf("member state statements = %#v", statements)
	}
	for _, statement := range statements {
		if strings.Contains(statement, "runnerfoundry_jobs") {
			t.Fatalf("capacity statement crossed job boundary: %s", statement)
		}
	}
}

func TestAvailableCapacityUsesOneSnapshot(t *testing.T) {
	t.Parallel()

	var arguments []any
	queryCalls := 0
	store := NewStore(Executor{queryRow: func(_ context.Context, _ string, values ...any) Row {
		queryCalls++
		arguments = values
		return rowFromValues([]any{30, 7, 5})
	}})
	capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", fixedTime())
	if err != nil || capacity != 18 || queryCalls != 1 || len(arguments) != 2 || arguments[0] != "linux" {
		t.Fatalf("available capacity = %d, queries=%d arguments=%#v error=%v",
			capacity, queryCalls, arguments, err)
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
				return assignDestinations(destinations, postgresPendingJobValues(result))
			}}
		},
		commit: func(context.Context) error { return nil }, rollback: func(context.Context) error { return nil },
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }})
	receipt, err := store.Finalize(context.Background(), testJobID7, core.ResultDeliveryReported)
	if err != nil || receipt.Outcome != result.Outcome || len(statements) != 3 ||
		!strings.Contains(statements[1], "source_receipts") || !strings.Contains(statements[2], "DELETE") {
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
	if err := store.ExpireMember(context.Background(), testMemberIDA); err != nil {
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
	if err != nil || !strings.Contains(statement, "runner_pool = $4") || len(arguments) != 5 || arguments[3] != "linux" {
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

func postgresPendingJobValues(result core.Result) []any {
	return []any{testJobID7, "github", "request-7", "linux", "result_pending", time.Unix(0, 1).UTC(), invalidNullUUID(),
		sql.NullTime{}, sql.NullTime{}, sql.NullTime{}, sql.NullTime{},
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
