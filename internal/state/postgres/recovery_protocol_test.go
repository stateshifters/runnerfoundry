package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestClaimOldestRequestsClaimEvidenceInOneTransaction(t *testing.T) {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	claim := core.ProcessingClaim{MemberID: testMemberIDA, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues(queuedJobValues(testJobID1, request)), rowFromValues(queuedJobValues(testJobID1, request)),
	}}
	result, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", claim)
	if err != nil || result.Outcome != core.ClaimClaimed || result.Processing.ClaimedAt != claim.ClaimedAt ||
		result.Processing.InventoryEndpointID != claim.InventoryEndpointID || protocol.commitCalls != 1 {
		t.Fatalf("claim = (%#v, %v), commits=%d", result, err, protocol.commitCalls)
	}
	last := len(protocol.statements) - 1
	if !strings.Contains(protocol.statements[last], "runner_name, claimed_at, inventory_endpoint_id") ||
		len(protocol.arguments[last]) != 8 {
		t.Fatalf("claim statement = %q %#v", protocol.statements[last], protocol.arguments[last])
	}
}

func TestClaimOldestReportsEveryTransactionFailure(t *testing.T) {
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{}); err == nil {
		t.Fatal("claim begin failure was ignored")
	}
	request := recoveryRequest()
	for name, failureCall := range map[string]int{"source lock": 1, "processing insertion": 2, "queue removal": 3} {
		t.Run(name, func(t *testing.T) {
			assertClaimExecuteFailure(t, request, failureCall, protocolErr)
		})
	}
	protocol := &recordedTransaction{rows: []Row{{scan: func(...any) error { return protocolErr }}}}
	if _, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{}); err == nil {
		t.Fatal("claim candidate failure was ignored")
	}
	protocol = &recordedTransaction{rows: []Row{rowFromValues(queuedJobValues(testJobID1, request))}}
	if result, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{}); err != nil || result.Outcome != core.ClaimEmpty {
		t.Fatalf("lost claim = (%#v, %v)", result, err)
	}
	protocol = &recordedTransaction{rows: recoveryQueuedRows(request), commitErr: protocolErr}
	if _, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{}); err == nil {
		t.Fatal("claim commit failure was ignored")
	}
}

func TestProcessingByPoolReportsEveryTransactionFailure(t *testing.T) {
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }}).
		ProcessingByPool(context.Background(), "linux"); err == nil {
		t.Fatal("pool lookup begin failure was ignored")
	}
	for _, testCase := range namedFailingListTransactions(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				ProcessingByPool(context.Background(), "linux"); err == nil {
				t.Fatal("pool lookup failure was ignored")
			}
		})
	}
}

func TestRecoverUnstartedReportsEveryTransactionFailure(t *testing.T) {
	protocolErr := errors.New("protocol")
	processing := recoveryProcessing()
	request := recoveryRequest()
	if _, err := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil {
		t.Fatal("recovery begin failure was ignored")
	}
	for _, transaction := range []Transaction{
		{execute: failingExecute(protocolErr), rollback: successfulCompletion},
		{execute: successfulExecute, queryRow: recoveryConstantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion},
		{execute: transactionFailingExecute(2, protocolErr).execute,
			queryRow: recoveryConstantRow(rowFromValues(queuedJobValues(testJobID1, request))), rollback: successfulCompletion},
		{execute: successfulExecute, queryRow: recoveryConstantRow(rowFromValues(queuedJobValues(testJobID1, request))),
			commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	} {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RecoverUnstartedProcessing(context.Background(), processing); err == nil {
			t.Fatal("recovery transaction failure was ignored")
		}
	}
}

func assertClaimExecuteFailure(t *testing.T, request core.JobRequest, failureCall int, protocolErr error) {
	t.Helper()
	calls := 0
	protocol := &recordedTransaction{rows: recoveryQueuedRows(request)}
	base, beginErr := protocol.executor().begin(context.Background())
	if beginErr != nil {
		t.Fatalf("begin recorded transaction: %v", beginErr)
	}
	base.execute = func(ctx context.Context, statement string, args ...any) (Result, error) {
		calls++
		if calls == failureCall {
			return Result{}, protocolErr
		}
		protocol.record(statement, args)
		return successfulExecute(ctx, statement, args...)
	}
	if _, err := NewStore(Executor{begin: transactionBeginning(base)}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{}); err == nil {
		t.Fatal("claim execute failure was ignored")
	}
}

func recoveryProcessing() core.ProcessingJob {
	return core.ProcessingJob{Job: core.Job{ID: testJobID1,
		Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux", EnqueuedAt: fixedTime()},
		ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
}

func recoveryRequest() core.JobRequest {
	return core.JobRequest{Source: recoveryProcessing().Job.Source, RunnerPool: "linux", EnqueuedAt: fixedTime()}
}

func recoveryQueuedRows(request core.JobRequest) []Row {
	return []Row{rowFromValues(queuedJobValues(testJobID1, request)), rowFromValues(queuedJobValues(testJobID1, request))}
}

func recoveryConstantRow(row Row) func(context.Context, string, ...any) Row {
	return func(context.Context, string, ...any) Row { return row }
}

func TestClaimOldestCommitsEmptySelection(t *testing.T) {
	protocol := &recordedTransaction{rows: []Row{{scan: func(...any) error { return sql.ErrNoRows }}}}
	result, err := NewStore(protocol.executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{})
	if err != nil || result.Outcome != core.ClaimEmpty || protocol.commitCalls != 1 {
		t.Fatalf("empty claim = (%#v, %v), commits=%d", result, err, protocol.commitCalls)
	}
}

func TestProcessingByPoolReadsRecoveryEvidence(t *testing.T) {
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}
	values := []any{job.ID, string(job.Source.Adapter), string(job.Source.ID), "linux", testMemberIDA,
		string(core.AttemptRunnerName(job.ID, core.JobID{})), fixedTime(),
		core.InventoryEndpointID("docker:member-a"), false, false}
	protocol := &recordedTransaction{queryRows: rowsFromValues([][]any{values})}
	processing, err := NewStore(protocol.executor()).ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].ClaimedAt != fixedTime() ||
		processing[0].InventoryEndpointID != "docker:member-a" || protocol.commitCalls != 1 {
		t.Fatalf("processing = (%#v, %v), commits=%d", processing, err, protocol.commitCalls)
	}
}

func TestRecoverUnstartedProcessingRequestsGuardedTransition(t *testing.T) {
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime().Add(time.Minute)}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	request := core.JobRequest{Source: processing.Job.Source, RunnerPool: "linux", EnqueuedAt: processing.Job.EnqueuedAt}
	protocol := &recordedTransaction{rows: []Row{rowFromValues(queuedJobValues(testJobID1, request))}}
	result, err := NewStore(protocol.executor()).RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryRequeued || protocol.commitCalls != 1 || len(protocol.statements) != 3 ||
		!strings.Contains(protocol.statements[1], "claimed_at = $2") ||
		!strings.Contains(protocol.statements[1], "NOT processing.runner_started") ||
		!strings.Contains(protocol.statements[1], "source_tombstones") {
		t.Fatalf("recovery = (%#v, %v), statements=%#v", result, err, protocol.statements)
	}

	protocol = &recordedTransaction{rows: []Row{{scan: func(...any) error { return sql.ErrNoRows }}}}
	result, err = NewStore(protocol.executor()).RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryMissing || protocol.commitCalls != 1 {
		t.Fatalf("raced recovery = (%#v, %v), commits=%d", result, err, protocol.commitCalls)
	}
}

func TestNamedStoreResultsDescribeLegacyProtocolOutcomes(t *testing.T) {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	queueProtocol := &recordedTransaction{rows: []Row{rowFromValues(queuedJobValues(testJobID1, request))}}
	queued, err := NewStore(queueProtocol.executor()).Queue(context.Background(), request)
	if err != nil || queued.Outcome != core.QueueQueued || queued.Job.ID != testJobID1 {
		t.Fatalf("queue = (%#v, %v)", queued, err)
	}
	transitionProtocol := &recordedTransaction{}
	store := NewStore(transitionProtocol.executor())
	if result, err := store.RemoveTerminal(context.Background(), testJobID1); err != nil || result.Outcome != core.TerminalRemoved {
		t.Fatalf("remove = (%#v, %v)", result, err)
	}
}
