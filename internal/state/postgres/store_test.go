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

func TestEnqueueCreatesQueuedJob(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	protocol := &recordedTransaction{rows: []Row{rowFromValues(queuedJobValues(testJobID1, request))}}
	job, created, err := NewStore(protocol.executor()).Enqueue(context.Background(), request)
	if err != nil || !created || job.ID != testJobID1 || protocol.commitCalls != 1 {
		t.Fatalf("enqueue = %#v, %t, commits=%d, error=%v", job, created, protocol.commitCalls, err)
	}
	if len(protocol.statements) != 2 || !strings.Contains(protocol.statements[0], "pg_advisory_xact_lock") ||
		!strings.Contains(protocol.statements[1], "runnerfoundry_queued_jobs") || len(protocol.arguments[1]) != 4 {
		t.Fatalf("enqueue protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestEnqueueReturnsExistingQueuedJob(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	protocol := &recordedTransaction{rows: []Row{
		{scan: func(...any) error { return sql.ErrNoRows }},
		rowFromValues(queuedJobValues(testJobID1, request)),
	}}
	job, created, err := NewStore(protocol.executor()).Enqueue(context.Background(), request)
	if err != nil || created || job.ID != testJobID1 || protocol.commitCalls != 1 {
		t.Fatalf("redelivery = %#v, %t, commits=%d, error=%v", job, created, protocol.commitCalls, err)
	}
}

func TestEnqueueIgnoresProcessingOrTombstonedSource(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}
	protocol := &recordedTransaction{rows: []Row{
		{scan: func(...any) error { return sql.ErrNoRows }},
		{scan: func(...any) error { return sql.ErrNoRows }},
	}}
	job, created, err := NewStore(protocol.executor()).Enqueue(context.Background(), request)
	if err != nil || created || job != (core.Job{}) || protocol.commitCalls != 1 {
		t.Fatalf("ignored enqueue = %#v, %t, commits=%d, error=%v", job, created, protocol.commitCalls, err)
	}
}

func TestTakeOldestMovesQueueEntryAtomically(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues(queuedJobValues(testJobID1, request)),
	}}
	processing, taken, err := NewStore(protocol.executor()).TakeOldest(context.Background(), "linux", testMemberIDA)
	if err != nil || !taken || processing.Job.ID != testJobID1 || processing.MemberID != testMemberIDA ||
		protocol.commitCalls != 1 {
		t.Fatalf("take = %#v, %t, commits=%d, error=%v", processing, taken, protocol.commitCalls, err)
	}
	if len(protocol.statements) != 5 || !strings.Contains(protocol.statements[1], "ORDER BY enqueued_at LIMIT 1") ||
		strings.Contains(protocol.statements[1], "ORDER BY enqueued_at,") ||
		strings.Contains(protocol.statements[1], "FOR UPDATE") ||
		!strings.Contains(protocol.statements[2], "pg_advisory_xact_lock") ||
		!strings.Contains(protocol.statements[3], "DELETE FROM public.runnerfoundry_queued_jobs") ||
		!strings.Contains(protocol.statements[4], "runnerfoundry_processing_jobs") {
		t.Fatalf("take protocol = %#v", protocol.statements)
	}
}

func TestTakeOldestLeavesEmptyQueueUnchanged(t *testing.T) {
	t.Parallel()
	protocol := &recordedTransaction{rows: []Row{{scan: func(...any) error { return sql.ErrNoRows }}}}
	processing, taken, err := NewStore(protocol.executor()).TakeOldest(context.Background(), "linux", testMemberIDA)
	if err != nil || taken || processing != (core.ProcessingJob{}) || len(protocol.statements) != 2 ||
		protocol.commitCalls != 1 {
		t.Fatalf("empty take = %#v, %t, statements=%d, commits=%d, error=%v",
			processing, taken, len(protocol.statements), protocol.commitCalls, err)
	}
}

func TestProcessingByMemberReturnsCurrentOwnership(t *testing.T) {
	t.Parallel()
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}
	values := processingJobValues(job, testMemberIDA)
	values[5], values[6] = true, true
	protocol := &recordedTransaction{queryRows: rowsFromValues([][]any{values})}
	processing, err := NewStore(protocol.executor()).ProcessingByMember(context.Background(), testMemberIDA, "linux")
	if err != nil || len(processing) != 1 || processing[0].Job != job || processing[0].MemberID != testMemberIDA ||
		!processing[0].RunnerStarted || !processing[0].SourceTerminal || protocol.commitCalls != 1 ||
		len(protocol.statements) != 1 || !strings.Contains(protocol.statements[0], "source_tombstones") {
		t.Fatalf("processing = %#v, commits=%d, error=%v", processing, protocol.commitCalls, err)
	}
}

func TestRemoveTerminalProcessingDeletesOnlyTerminalJob(t *testing.T) {
	t.Parallel()
	protocol := &recordedTransaction{}
	removed, err := NewStore(protocol.executor()).RemoveTerminalProcessing(context.Background(), testJobID1)
	if err != nil || !removed || protocol.commitCalls != 1 || len(protocol.statements) != 1 ||
		len(protocol.arguments[0]) != 1 || protocol.arguments[0][0] != testJobID1 {
		t.Fatalf("remove statements=%#v arguments=%#v commits=%d error=%v",
			protocol.statements, protocol.arguments, protocol.commitCalls, err)
	}
}

func TestStoreOperationsReportProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	operations := []func(*Store) error{
		func(store *Store) error { _, _, err := store.Enqueue(context.Background(), request); return err },
		func(store *Store) error {
			_, _, err := store.TakeOldest(context.Background(), "linux", testMemberIDA)
			return err
		},
		func(store *Store) error { return store.TerminalizeSource(context.Background(), request.Source) },
		func(store *Store) error { return store.MarkRunnerStarted(context.Background(), request.Source) },
		func(store *Store) error {
			_, err := store.ProcessingByMember(context.Background(), testMemberIDA, "linux")
			return err
		},
		func(store *Store) error {
			_, err := store.RetryUnstartedProcessing(context.Background(), testJobID1)
			return err
		},
		func(store *Store) error {
			_, err := store.RemoveTerminalProcessing(context.Background(), testJobID1)
			return err
		},
	}
	for index, operation := range operations {
		store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }})
		if err := operation(store); err == nil {
			t.Fatalf("operation %d: expected begin error", index)
		}
	}
}

type recordedTransaction struct {
	statements  []string
	arguments   [][]any
	rows        []Row
	queryRows   Rows
	executeErr  error
	commitErr   error
	commitCalls int
	rollbackErr error
}

func (protocol *recordedTransaction) executor() Executor {
	transaction := Transaction{
		execute: func(_ context.Context, statement string, arguments ...any) (Result, error) {
			protocol.record(statement, arguments)
			if protocol.executeErr != nil {
				return Result{}, protocol.executeErr
			}
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		queryRow: func(_ context.Context, statement string, arguments ...any) Row {
			protocol.record(statement, arguments)
			if len(protocol.rows) == 0 {
				return Row{scan: func(...any) error { return sql.ErrNoRows }}
			}
			row := protocol.rows[0]
			protocol.rows = protocol.rows[1:]
			return row
		},
		query: func(_ context.Context, statement string, arguments ...any) (Rows, error) {
			protocol.record(statement, arguments)
			return protocol.queryRows, nil
		},
		commit:   func(context.Context) error { protocol.commitCalls++; return protocol.commitErr },
		rollback: func(context.Context) error { return protocol.rollbackErr },
	}
	return Executor{begin: func(context.Context) (Transaction, error) { return transaction, nil }}
}

func (protocol *recordedTransaction) record(statement string, arguments []any) {
	protocol.statements = append(protocol.statements, statement)
	protocol.arguments = append(protocol.arguments, arguments)
}

func queuedJobValues(identifier core.JobID, request core.JobRequest) []any {
	return []any{identifier, request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt}
}

func processingJobValues(job core.Job, memberID core.MemberID) []any {
	return []any{job.ID, job.Source.Adapter, job.Source.ID, string(job.RunnerPool), memberID, false, false}
}

func rowFromValues(values []any) Row {
	return Row{scan: func(destinations ...any) error { return assignDestinations(destinations, values) }}
}

func rowsFromValues(values [][]any) Rows {
	index := -1
	return Rows{close: func() error { return nil }, err: func() error { return nil },
		next: func() bool { index++; return index < len(values) },
		scan: func(destinations ...any) error { return assignDestinations(destinations, values[index]) }}
}

func assignDestinations(destinations, values []any) error {
	if len(destinations) != len(values) {
		return fmt.Errorf("destination count %d does not match value count %d", len(destinations), len(values))
	}
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

func fixedTime() time.Time {
	return time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC)
}
