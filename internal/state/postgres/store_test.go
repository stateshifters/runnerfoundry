package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

func TestEnqueueCreatesQueuedJob(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	protocol := &recordedTransaction{rows: []Row{rowFromValues(queuedJobValues(testJobID1, request))}}
	result, err := NewStore(protocol.executor()).Queue(context.Background(), request)
	if err != nil || result.Outcome != core.QueueQueued || result.Job.ID != testJobID1 || protocol.commitCalls != 1 {
		t.Fatalf("queue = %#v, commits=%d, error=%v", result, protocol.commitCalls, err)
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
	result, err := NewStore(protocol.executor()).Queue(context.Background(), request)
	if err != nil || result.Outcome != core.QueueAlreadyQueued || result.Job.ID != testJobID1 || protocol.commitCalls != 1 {
		t.Fatalf("redelivery = %#v, commits=%d, error=%v", result, protocol.commitCalls, err)
	}
}

func TestEnqueueIgnoresProcessingOrTombstonedSource(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}
	protocol := &recordedTransaction{rows: []Row{
		{scan: func(...any) error { return sql.ErrNoRows }},
		{scan: func(...any) error { return sql.ErrNoRows }},
		rowFromValues([]any{string(core.QueueProcessing)}),
	}}
	result, err := NewStore(protocol.executor()).Queue(context.Background(), request)
	if err != nil || result.Outcome != core.QueueProcessing || result.Job != (core.Job{}) || protocol.commitCalls != 1 {
		t.Fatalf("ignored queue = %#v, commits=%d, error=%v", result, protocol.commitCalls, err)
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
	result, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA})
	if err != nil || result.Outcome != core.ClaimClaimed || result.Processing.Job.ID != testJobID1 || result.Processing.MemberID != testMemberIDA ||
		protocol.commitCalls != 1 {
		t.Fatalf("claim = %#v, commits=%d, error=%v", result, protocol.commitCalls, err)
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
	result, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA})
	if err != nil || result.Outcome != core.ClaimEmpty || result.Processing != (core.ProcessingJob{}) || len(protocol.statements) != 2 ||
		protocol.commitCalls != 1 {
		t.Fatalf("empty take = %#v, %s, statements=%d, commits=%d, error=%v",
			result.Processing, result.Outcome, len(protocol.statements), protocol.commitCalls, err)
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
	result, err := NewStore(protocol.executor()).RemoveTerminal(context.Background(), testJobID1)
	if err != nil || result.Outcome != core.TerminalRemoved || protocol.commitCalls != 1 || len(protocol.statements) != 1 ||
		len(protocol.arguments[0]) != 1 || protocol.arguments[0][0] != testJobID1 {
		t.Fatalf("remove statements=%#v arguments=%#v commits=%d error=%v",
			protocol.statements, protocol.arguments, protocol.commitCalls, err)
	}
}

func TestStoreOperationsReportProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	for _, testCase := range protocolFailureOperations(request) {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }})
			if err := testCase.operation(store); err == nil {
				t.Fatal("expected begin error")
			}
		})
	}
}

type namedStoreOperation struct {
	name      string
	operation func(*Store) error
}

func protocolFailureOperations(request core.JobRequest) []namedStoreOperation {
	return []namedStoreOperation{
		{name: "queue", operation: func(store *Store) error { _, err := store.Queue(context.Background(), request); return err }},
		{name: "claim", operation: func(store *Store) error {
			_, err := store.ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA})
			return err
		}},
		{name: "terminalize source", operation: func(store *Store) error { return store.TerminalizeSource(context.Background(), request.Source) }},
		{name: "mark runner started", operation: func(store *Store) error { return store.MarkRunnerStarted(context.Background(), request.Source) }},
		{name: "processing list", operation: func(store *Store) error {
			_, err := store.ProcessingByMember(context.Background(), testMemberIDA, "linux")
			return err
		}},
		{name: "retry", operation: func(store *Store) error {
			_, err := store.RetryUnstarted(context.Background(), testJobID1)
			return err
		}},
		{name: "remove terminal", operation: func(store *Store) error {
			_, err := store.RemoveTerminal(context.Background(), testJobID1)
			return err
		}},
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
	return []any{identifier, string(request.Source.Adapter), string(request.Source.ID), string(request.RunnerPool), request.EnqueuedAt}
}

func processingJobValues(job core.Job, memberID core.MemberID) []any {
	return []any{job.ID, string(job.Source.Adapter), string(job.Source.ID), string(job.RunnerPool), memberID, false, false}
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
		if !assignTestValue(destinations[index], value) {
			return fmt.Errorf("cannot assign %T to %T", value, destinations[index])
		}
	}
	return nil
}

func assignTestValue(destination, value any) bool {
	if assignScalarTestValue(destination, value) {
		return true
	}
	switch target := destination.(type) {
	case *time.Time:
		assigned, ok := value.(time.Time)
		*target = assigned
		return ok
	case *uuid.UUID:
		assigned, ok := value.(uuid.UUID)
		*target = assigned
		return ok
	case *core.MemberID:
		assigned, ok := value.(core.MemberID)
		*target = assigned
		return ok
	case *core.InventoryEndpointID:
		assigned, ok := value.(core.InventoryEndpointID)
		*target = assigned
		return ok
	default:
		return false
	}
}

func assignScalarTestValue(destination, value any) bool {
	switch target := destination.(type) {
	case *string:
		assigned, ok := value.(string)
		*target = assigned
		return ok
	case *int:
		assigned, ok := value.(int)
		*target = assigned
		return ok
	case *int64:
		assigned, ok := value.(int64)
		*target = assigned
		return ok
	case *bool:
		assigned, ok := value.(bool)
		*target = assigned
		return ok
	default:
		return false
	}
}

func fixedTime() time.Time {
	return time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC)
}
