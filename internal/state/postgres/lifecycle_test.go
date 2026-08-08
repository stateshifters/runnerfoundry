package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestTerminalizeSourceTombstonesAndDeletesQueued(t *testing.T) {
	t.Parallel()
	protocol := &recordedTransaction{}
	result, err := NewStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{
		Source: core.SourceReference{Adapter: "github", ID: "request-1"},
	})
	if err != nil || result.Outcome != core.ObservationApplied || protocol.commitCalls != 1 || len(protocol.statements) != 3 ||
		!strings.Contains(protocol.statements[1], "runnerfoundry_source_tombstones") ||
		!strings.Contains(protocol.statements[2], "runnerfoundry_queued_jobs") {
		t.Fatalf("terminalize result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
}

func TestMarkRunnerStartedUpdatesProcessingWithoutTombstoning(t *testing.T) {
	t.Parallel()
	protocol := &recordedTransaction{}
	result, err := NewStore(protocol.executor()).MarkRunnerStarted(context.Background(), core.JobIdentity{
		Source: core.SourceReference{Adapter: "github", ID: "request-1"},
	})
	if err != nil || result.Outcome != core.ObservationApplied || protocol.commitCalls != 1 || len(protocol.statements) != 4 ||
		!strings.Contains(protocol.statements[1], "runner_started = true") ||
		!strings.Contains(protocol.statements[2], "source_tombstones") ||
		!strings.Contains(protocol.statements[3], "queued_jobs") {
		t.Fatalf("start result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
}

func TestTerminalizeResolvesJobIdentityBeforeTombstoning(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
	}}
	result, err := NewStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
	if err != nil || result.Outcome != core.ObservationApplied || protocol.commitCalls != 1 || len(protocol.statements) != 5 {
		t.Fatalf("terminalize job result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
	if protocol.arguments[2][0] != testJobID1 || protocol.arguments[3][0] != source.Adapter {
		t.Fatalf("terminalize arguments=%#v", protocol.arguments)
	}
}

func TestLifecycleObservationRejectsMismatchedSourceAndJobWithoutMutation(t *testing.T) {
	t.Parallel()
	resolved := core.SourceReference{Adapter: "github", ID: "request-1"}
	provided := core.SourceReference{Adapter: "github", ID: "request-2"}
	for name, operation := range map[string]func(*Store) (core.ObservationResult, error){
		"started": func(store *Store) (core.ObservationResult, error) {
			return store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: provided, JobID: testJobID1})
		},
		"terminal": func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{Source: provided, JobID: testJobID1})
		},
	} {
		t.Run(name, func(t *testing.T) {
			protocol := &recordedTransaction{rows: []Row{rowFromValues([]any{string(resolved.Adapter), string(resolved.ID)})}}
			result, err := operation(NewStore(protocol.executor()))
			if err == nil || result != (core.ObservationResult{}) || protocol.commitCalls != 0 || len(protocol.statements) != 1 {
				t.Fatalf("%s result=%#v statements=%#v commits=%d error=%v", name, result, protocol.statements, protocol.commitCalls, err)
			}
		})
	}
}

func TestJobOnlyLifecycleObservationIsUnmatchedWhenDurableJobIsMissing(t *testing.T) {
	t.Parallel()
	for name, operation := range map[string]func(*Store) (core.ObservationResult, error){
		"started": func(store *Store) (core.ObservationResult, error) {
			return store.MarkRunnerStarted(context.Background(), core.JobIdentity{JobID: testJobID1})
		},
		"terminal": func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
		},
	} {
		t.Run(name, func(t *testing.T) {
			protocol := &recordedTransaction{}
			result, err := operation(NewStore(protocol.executor()))
			if err != nil || result.Outcome != core.ObservationUnmatched || protocol.commitCalls != 1 || len(protocol.statements) != 1 {
				t.Fatalf("%s result=%#v statements=%#v commits=%d error=%v", name, result, protocol.statements, protocol.commitCalls, err)
			}
		})
	}
}

func TestTerminalizeClassifiesReplayAsAlreadyApplied(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	exerciseSourceObservationOutcome(t, nil, 0, core.ObservationAlreadyApplied,
		func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{Source: source})
		})
}

func TestMarkRunnerStartedClassifiesReplayAsAlreadyApplied(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	exerciseSourceObservationOutcome(t, []Row{rowFromValues([]any{true})}, 0, core.ObservationAlreadyApplied,
		func(store *Store) (core.ObservationResult, error) {
			return store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: source})
		})
}

func TestMarkRunnerStartedClassifiesMissingSourceStateAsUnmatched(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	exerciseSourceObservationOutcome(t, []Row{rowFromValues([]any{false})}, 0, core.ObservationUnmatched,
		func(store *Store) (core.ObservationResult, error) {
			return store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: source})
		})
}

func exerciseSourceObservationOutcome(
	t *testing.T,
	rows []Row,
	affected int64,
	want core.ObservationOutcome,
	operation func(*Store) (core.ObservationResult, error),
) {
	t.Helper()
	commits := 0
	transaction := postgresOutcomeTransaction(rows, affected, &commits)
	result, err := operation(NewStore(Executor{begin: transactionBeginning(transaction)}))
	if err != nil || result.Outcome != want || commits != 1 {
		t.Fatalf("result=%#v commits=%d error=%v", result, commits, err)
	}
}

func TestMarkRunnerStartedAcceptsMatchingSourceAndJobIdentity(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
	}}
	result, err := NewStore(protocol.executor()).MarkRunnerStarted(context.Background(), core.JobIdentity{
		Source: source, JobID: testJobID1,
	})
	if err != nil || result.Outcome != core.ObservationApplied || protocol.commitCalls != 1 {
		t.Fatalf("result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
}

func TestRetryUnstartedProcessingMovesSameJobToDatabaseTimedQueueTail(t *testing.T) {
	t.Parallel()
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}
	queued := core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool, EnqueuedAt: fixedTime()}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues(processingJobValues(job, testMemberIDA)),
		rowFromValues(queuedJobValues(testJobID1, queued)),
	}}
	result, err := NewStore(protocol.executor()).RetryUnstarted(context.Background(), testJobID1)
	if err != nil || result.Outcome != core.RetryRequeued || protocol.commitCalls != 1 || len(protocol.statements) != 4 ||
		len(protocol.arguments[2]) != 1 || !strings.Contains(protocol.statements[2], "RETURNING") ||
		strings.Contains(protocol.statements[2], "enqueued_at) VALUES") {
		t.Fatalf("retry statements=%#v arguments=%#v commits=%d result=%#v error=%v",
			protocol.statements, protocol.arguments, protocol.commitCalls, result, err)
	}
}
