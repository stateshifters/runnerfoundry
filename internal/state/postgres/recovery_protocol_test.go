package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestClaimOldestRequestsClaimEvidenceInOneTransaction(t *testing.T) {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	claim := core.ProcessingClaim{MemberID: testMemberIDA, AttemptID: core.AttemptID{1}, ClaimedAt: fixedTime(),
		InventoryEndpointID: "docker:member-a"}
	protocol := &recordedTransaction{rows: []rowStub{
		rowFromValues(queuedJobValues(testJobID1, request)), rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues([]any{claim.ClaimedAt}),
	}}
	result, err := newStore(protocol.executor()).ClaimOldest(context.Background(), "linux", claim)
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
	if _, err := newStore(executorStub{begin: func(context.Context) (transactionStub, error) { return transactionStub{}, protocolErr }}).
		ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("claim begin failure was ignored")
	}
	request := recoveryRequest()
	for name, failureCall := range map[string]int{"Runner Pool lock": 1, "source lock": 2} {
		t.Run(name, func(t *testing.T) {
			assertClaimExecuteFailure(t, request, failureCall, protocolErr)
		})
	}
	protocol := &recordedTransaction{rows: []rowStub{rowStub{scan: func(...any) error { return protocolErr }}}}
	if _, err := newStore(protocol.executor()).ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("claim candidate failure was ignored")
	}
	protocol = &recordedTransaction{rows: []rowStub{rowFromValues(queuedJobValues(testJobID1, request))}}
	if result, err := newStore(protocol.executor()).ClaimOldest(context.Background(), "linux", validProcessingClaim()); err != nil || result.Outcome != core.ClaimEmpty {
		t.Fatalf("lost claim = (%#v, %v)", result, err)
	}
	protocol = &recordedTransaction{rows: recoveryQueuedRows(request), commitErr: protocolErr}
	if _, err := newStore(protocol.executor()).ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("claim commit failure was ignored")
	}
}

func TestClaimOldestReportsPersistedClaimReadFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := recoveryRequest()
	protocol := &recordedTransaction{rows: []rowStub{
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues(queuedJobValues(testJobID1, request)),
		{scan: func(...any) error { return protocolErr }},
	}}
	if _, err := newStore(protocol.executor()).ClaimOldest(
		context.Background(),
		"linux",
		validProcessingClaim(),
	); !errors.Is(err, protocolErr) {
		t.Fatalf("claim insertion error = %v", err)
	}
}

func TestClaimOldestRequiresCallerOwnedAttemptIdentityBeforePersistence(t *testing.T) {
	protocol := &recordedTransaction{}
	claim := validProcessingClaim()
	claim.AttemptID = core.AttemptID{}
	result, err := newStore(protocol.executor()).ClaimOldest(context.Background(), "linux", claim)
	if !errors.Is(err, core.ErrInvalidProcessingClaim) || result.Outcome != "" || len(protocol.statements) != 0 {
		t.Fatalf("ClaimOldest() = (%#v, %v), statements = %#v", result, err, protocol.statements)
	}
}

func TestProcessingByPoolReportsEveryTransactionFailure(t *testing.T) {
	protocolErr := errors.New("protocol")
	if _, err := newStore(executorStub{begin: func(context.Context) (transactionStub, error) { return transactionStub{}, protocolErr }}).
		ProcessingByPool(context.Background(), "linux"); err == nil {
		t.Fatal("pool lookup begin failure was ignored")
	}
	for _, testCase := range namedFailingListTransactions(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).
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
	if _, err := newStore(executorStub{begin: func(context.Context) (transactionStub, error) { return transactionStub{}, protocolErr }}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil {
		t.Fatal("recovery begin failure was ignored")
	}
	for _, transaction := range []transactionStub{
		{execute: failingExecute(protocolErr), rollback: successfulCompletion},
		{execute: successfulExecute, queryRow: recoveryConstantRow(rowStub{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion},
		{execute: transactionFailingExecute(2, protocolErr).execute,
			queryRow: recoveryConstantRow(rowFromValues(queuedJobValues(testJobID1, request))), rollback: successfulCompletion},
		{execute: successfulExecute, queryRow: recoveryConstantRow(rowFromValues(queuedJobValues(testJobID1, request))),
			commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	} {
		if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
			RecoverUnstartedProcessing(context.Background(), processing); err == nil {
			t.Fatal("recovery transaction failure was ignored")
		}
	}
}

func assertClaimExecuteFailure(t *testing.T, request core.JobRequest, failureCall int, protocolErr error) {
	t.Helper()
	calls := 0
	protocol := &recordedTransaction{rows: recoveryQueuedRows(request)}
	base := protocol.transaction()
	base.execute = func(ctx context.Context, statement string, args ...any) (resultStub, error) {
		calls++
		if calls == failureCall {
			return resultStub{}, protocolErr
		}
		protocol.record(statement, args)
		return successfulExecute(ctx, statement, args...)
	}
	if _, err := newStore(executorStub{begin: transactionBeginning(base)}).
		ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
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

func recoveryQueuedRows(request core.JobRequest) []rowStub {
	return []rowStub{
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues([]any{validProcessingClaim().ClaimedAt}),
	}
}

func recoveryConstantRow(row rowStub) func(context.Context, string, ...any) rowStub {
	return func(context.Context, string, ...any) rowStub { return row }
}

func TestClaimOldestCommitsEmptySelection(t *testing.T) {
	protocol := &recordedTransaction{rows: []rowStub{rowStub{scan: func(...any) error { return sql.ErrNoRows }}}}
	result, err := newStore(protocol.executor()).ClaimOldest(context.Background(), "linux", validProcessingClaim())
	if err != nil || result.Outcome != core.ClaimEmpty || protocol.commitCalls != 1 {
		t.Fatalf("empty claim = (%#v, %v), commits=%d", result, err, protocol.commitCalls)
	}
}

func TestProcessingByPoolReadsRecoveryEvidence(t *testing.T) {
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}
	values := []any{job.ID, string(job.Source.Adapter), string(job.Source.ID), "linux", testMemberIDA,
		string(core.AttemptRunnerName(job.ID, core.AttemptID{1})), fixedTime(),
		core.InventoryEndpointID("docker:member-a"), false, false, ""}
	protocol := &recordedTransaction{queryRows: rowsFromValues([][]any{values})}
	processing, err := newStore(protocol.executor()).ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].ClaimedAt != fixedTime() ||
		processing[0].InventoryEndpointID != "docker:member-a" || protocol.commitCalls != 1 {
		t.Fatalf("processing = (%#v, %v), commits=%d", processing, err, protocol.commitCalls)
	}
}

func TestRecoverUnstartedProcessingRequestsGuardedTransition(t *testing.T) {
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: fixedTime().Add(time.Minute)},
		RunnerName: core.AttemptRunnerName(testJobID1, core.AttemptID{1}),
		ClaimedAt:  fixedTime(), InventoryEndpointID: "docker:member-a"}
	request := core.JobRequest{Source: processing.Job.Source, RunnerPool: "linux", EnqueuedAt: processing.Job.EnqueuedAt}
	protocol := &recordedTransaction{rows: []rowStub{rowFromValues(queuedJobValues(testJobID1, request))}}
	result, err := newStore(protocol.executor()).RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryRequeued || protocol.commitCalls != 1 {
		t.Fatalf("recovery = (%#v, %v), statements=%#v arguments=%#v",
			result, err, protocol.statements, protocol.arguments)
	}
	assertPostgresRecoveryAttemptGuard(t, protocol, processing.RunnerName)

	protocol = &recordedTransaction{rows: []rowStub{rowStub{scan: func(...any) error { return sql.ErrNoRows }}}}
	result, err = newStore(protocol.executor()).RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryMissing || protocol.commitCalls != 1 {
		t.Fatalf("raced recovery = (%#v, %v), commits=%d", result, err, protocol.commitCalls)
	}
}

func assertPostgresRecoveryAttemptGuard(
	t *testing.T,
	protocol *recordedTransaction,
	runnerName core.RunnerName,
) {
	t.Helper()
	if len(protocol.statements) != 3 || len(protocol.arguments[1]) != 5 || len(protocol.arguments[2]) != 4 {
		t.Fatalf("recovery protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
	for index, fragment := range map[int]string{
		1: "claimed_at = $2", 2: "runner_name = $4",
	} {
		if !strings.Contains(protocol.statements[index], fragment) {
			t.Fatalf("recovery statement %d = %q, want %q", index, protocol.statements[index], fragment)
		}
	}
	for _, fragment := range []string{"NOT processing.runner_started", "source_tombstones", "runner_name = $5"} {
		if !strings.Contains(protocol.statements[1], fragment) {
			t.Fatalf("recovery insert = %q, want %q", protocol.statements[1], fragment)
		}
	}
	if protocol.arguments[1][4] != runnerName || protocol.arguments[2][3] != runnerName {
		t.Fatalf("recovery arguments = %#v, want runner %q", protocol.arguments, runnerName)
	}
}

func TestNamedStoreResultsDescribeTransitionOutcomes(t *testing.T) {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	queueProtocol := &recordedTransaction{rows: []rowStub{rowFromValues(queuedJobValues(testJobID1, request))}}
	queued, err := newStore(queueProtocol.executor()).Queue(context.Background(), request)
	if err != nil || queued.Outcome != core.QueueQueued || queued.Job.ID != testJobID1 {
		t.Fatalf("queue = (%#v, %v)", queued, err)
	}
	transitionProtocol := &recordedTransaction{}
	store := newStore(transitionProtocol.executor())
	if result, err := store.RemoveTerminal(context.Background(), testJobID1); err != nil || result.Outcome != core.TerminalRemoved {
		t.Fatalf("remove = (%#v, %v)", result, err)
	}
}
