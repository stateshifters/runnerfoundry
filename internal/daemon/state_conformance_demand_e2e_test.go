//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

func exerciseDemandTransactionContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	fixture := prepareDemandTransactionFixture(t, store)
	ctx := context.Background()
	err := store.WithDemandTransaction(ctx, fixture.key, func(transaction statecontract.DemandTransaction) error {
		return removeDemandTransactionJobs(ctx, t, transaction, fixture)
	})
	if err != nil {
		t.Fatalf("commit typed demand transaction: %v", err)
	}
	requireQueued(t, store, fixture.queuedRequest, core.QueueQueued)
	exerciseDemandTransactionRollback(t, store, fixture.key.RunnerPool)
}

type demandTransactionFixture struct {
	key           statecontract.DemandTransactionKey
	processing    core.ProcessingJob
	queuedJob     core.Job
	queuedRequest core.JobRequest
}

func prepareDemandTransactionFixture(t *testing.T, store stateConformanceStore) demandTransactionFixture {
	t.Helper()
	runnerPool := core.RunnerPool("state-conformance-demand")
	processingRequest := stateConformanceRequest("demand-processing", runnerPool, stateConformanceEpoch.Add(2*time.Hour))
	queuedRequest := stateConformanceRequest("demand-queued", runnerPool, stateConformanceEpoch.Add(2*time.Hour+time.Second))
	processingJob := requireQueued(t, store, processingRequest, core.QueueQueued)
	queuedJob := requireQueued(t, store, queuedRequest, core.QueueQueued)
	claimedAt := stateConformanceEpoch.Add(2*time.Hour + 2*time.Second)
	requireStateConformanceMember(t, store, "demand-member", claimedAt)
	processing := requireClaimed(t, store, runnerPool, stateConformanceClaim("demand-member", claimedAt))
	if processing.Job.ID != processingJob.ID {
		t.Fatalf("demand processing job = %s, want %s", processing.Job.ID, processingJob.ID)
	}
	scope := core.DemandScope{Adapter: "state-conformance", ID: "typed-active-demand"}
	return demandTransactionFixture{
		key:           statecontract.DemandTransactionKey{Scope: scope, RunnerPool: runnerPool},
		processing:    processing,
		queuedJob:     queuedJob,
		queuedRequest: queuedRequest,
	}
}

func removeDemandTransactionJobs(
	ctx context.Context,
	t *testing.T,
	transaction statecontract.DemandTransaction,
	fixture demandTransactionFixture,
) error {
	t.Helper()
	activeJobs, err := transaction.ActiveJobs(ctx)
	if err != nil {
		return fmt.Errorf("list active jobs before demand removal: %w", err)
	}
	assertStateConformanceActiveJobs(t, activeJobs, fixture.processing, fixture.queuedJob, fixture.queuedRequest.EnqueuedAt)
	processingRemoval, removalErr := transaction.RemoveQueued(ctx, activeJobByID(t, activeJobs, fixture.processing.Job.ID))
	if removalErr != nil || processingRemoval != statecontract.QueuedJobMissing {
		t.Fatalf("processing demand removal = %q, error = %v", processingRemoval, removalErr)
	}
	queued := activeJobByID(t, activeJobs, fixture.queuedJob.ID)
	removed, removalErr := transaction.RemoveQueued(ctx, queued)
	if removalErr != nil || removed != statecontract.QueuedJobRemoved {
		t.Fatalf("queued demand removal = %q, error = %v", removed, removalErr)
	}
	removed, removalErr = transaction.RemoveQueued(ctx, queued)
	if removalErr != nil || removed != statecontract.QueuedJobMissing {
		t.Fatalf("repeated queued demand removal = %q, error = %v", removed, removalErr)
	}
	return nil
}

func assertStateConformanceActiveJobs(
	t *testing.T,
	jobs []statecontract.ActiveJob,
	processing core.ProcessingJob,
	queued core.Job,
	queuedAt time.Time,
) {
	t.Helper()
	if len(jobs) != 2 {
		t.Fatalf("active jobs = %#v, want one queued and one processing", jobs)
	}
	processingActive := activeJobByID(t, jobs, processing.Job.ID)
	if processingActive.Source != processing.Job.Source || processingActive.RunnerPool != processing.Job.RunnerPool ||
		processingActive.Phase != statecontract.ActiveJobProcessing || !processingActive.StateChangedAt.Equal(processing.ClaimedAt) {
		t.Fatalf("typed processing job = %#v, want processing state for %+v", processingActive, processing)
	}
	queuedActive := activeJobByID(t, jobs, queued.ID)
	if queuedActive.Source != queued.Source || queuedActive.RunnerPool != queued.RunnerPool ||
		queuedActive.Phase != statecontract.ActiveJobQueued || !queuedActive.StateChangedAt.Equal(queuedAt) {
		t.Fatalf("typed queued job = %#v, want queued state for %+v", queuedActive, queued)
	}
}

func activeJobByID(t *testing.T, jobs []statecontract.ActiveJob, jobID core.JobID) statecontract.ActiveJob {
	t.Helper()
	for _, job := range jobs {
		if job.ID == jobID {
			return job
		}
	}
	t.Fatalf("active job %s not found in %#v", jobID, jobs)
	return statecontract.ActiveJob{}
}

func exerciseDemandTransactionRollback(t *testing.T, store stateConformanceStore, runnerPool core.RunnerPool) {
	t.Helper()
	ctx := context.Background()
	retainedRequest := stateConformanceRequest("rollback-retained", runnerPool, stateConformanceEpoch.Add(3*time.Hour))
	transientRequest := stateConformanceRequest("rollback-transient", runnerPool, stateConformanceEpoch.Add(3*time.Hour+time.Second))
	retained := requireQueued(t, store, retainedRequest, core.QueueQueued)
	wantErr := errors.New("roll back state conformance demand transaction")
	scope := core.DemandScope{Adapter: "state-conformance", ID: "rollback-demand"}
	transientRequest.Scope = scope
	err := store.WithDemandTransaction(ctx,
		statecontract.DemandTransactionKey{Scope: scope, RunnerPool: runnerPool},
		func(transaction statecontract.DemandTransaction) error {
			return rollBackDemandTransaction(ctx, t, transaction, transientRequest, retained.ID, wantErr)
		})
	if !errors.Is(err, wantErr) {
		t.Fatalf("demand transaction rollback error = %v, want %v", err, wantErr)
	}
	requireQueued(t, store, retainedRequest, core.QueueAlreadyQueued)
	requireQueued(t, store, transientRequest, core.QueueQueued)
}

func rollBackDemandTransaction(
	ctx context.Context,
	t *testing.T,
	transaction statecontract.DemandTransaction,
	transientRequest core.JobRequest,
	retainedJobID core.JobID,
	wantErr error,
) error {
	t.Helper()
	jobs, err := transaction.ActiveJobs(ctx)
	if err != nil {
		return fmt.Errorf("list active jobs before demand rollback: %w", err)
	}
	if result, queueErr := transaction.Queue(ctx, transientRequest); queueErr != nil || result.Outcome != core.QueueQueued {
		t.Fatalf("transactional queue before rollback = %+v, error = %v", result, queueErr)
	}
	removed, removalErr := transaction.RemoveQueued(ctx, activeJobByID(t, jobs, retainedJobID))
	if removalErr != nil || removed != statecontract.QueuedJobRemoved {
		t.Fatalf("transactional removal before rollback = %q, error = %v", removed, removalErr)
	}
	return wantErr
}
