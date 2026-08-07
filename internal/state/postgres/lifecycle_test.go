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
	err := NewStore(protocol.executor()).TerminalizeSource(context.Background(),
		core.SourceReference{Adapter: "github", ID: "request-1"})
	if err != nil || protocol.commitCalls != 1 || len(protocol.statements) != 3 ||
		!strings.Contains(protocol.statements[1], "runnerfoundry_source_tombstones") ||
		!strings.Contains(protocol.statements[2], "runnerfoundry_queued_jobs") {
		t.Fatalf("terminalize statements=%#v commits=%d error=%v", protocol.statements, protocol.commitCalls, err)
	}
}

func TestMarkRunnerStartedUpdatesProcessingWithoutTombstoning(t *testing.T) {
	t.Parallel()
	protocol := &recordedTransaction{}
	err := NewStore(protocol.executor()).MarkRunnerStarted(context.Background(),
		core.SourceReference{Adapter: "github", ID: "request-1"})
	if err != nil || protocol.commitCalls != 1 || len(protocol.statements) != 4 ||
		!strings.Contains(protocol.statements[1], "runner_started = true") ||
		!strings.Contains(protocol.statements[2], "source_tombstones") ||
		!strings.Contains(protocol.statements[3], "queued_jobs") {
		t.Fatalf("start statements=%#v commits=%d error=%v", protocol.statements, protocol.commitCalls, err)
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
