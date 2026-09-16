//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

func exerciseDemandTransactionContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	ctx := context.Background()
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
	err := store.WithDemandTransaction(ctx, statecontract.DemandTransactionKey{Scope: scope, RunnerPool: runnerPool}, func(transaction statecontract.DemandTransaction) error {
		activeJobs, activeErr := transaction.ActiveJobs(ctx)
		if activeErr != nil {
			return activeErr
		}
		assertStateConformanceActiveJobs(t, activeJobs, processing, queuedJob, queuedRequest.EnqueuedAt)
		processingRemoval, removalErr := transaction.RemoveQueued(ctx, activeJobByID(t, activeJobs, processing.Job.ID))
		if removalErr != nil || processingRemoval != statecontract.QueuedJobMissing {
			t.Fatalf("processing demand removal = %q, error = %v", processingRemoval, removalErr)
		}
		queued := activeJobByID(t, activeJobs, queuedJob.ID)
		removed, removalErr := transaction.RemoveQueued(ctx, queued)
		if removalErr != nil || removed != statecontract.QueuedJobRemoved {
			t.Fatalf("queued demand removal = %q, error = %v", removed, removalErr)
		}
		removed, removalErr = transaction.RemoveQueued(ctx, queued)
		if removalErr != nil || removed != statecontract.QueuedJobMissing {
			t.Fatalf("repeated queued demand removal = %q, error = %v", removed, removalErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("commit typed demand transaction: %v", err)
	}
	requireQueued(t, store, queuedRequest, core.QueueQueued)
	exerciseDemandTransactionRollback(t, store, runnerPool)
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
			jobs, activeErr := transaction.ActiveJobs(ctx)
			if activeErr != nil {
				return activeErr
			}
			if result, queueErr := transaction.Queue(ctx, transientRequest); queueErr != nil || result.Outcome != core.QueueQueued {
				t.Fatalf("transactional queue before rollback = %+v, error = %v", result, queueErr)
			}
			removed, removalErr := transaction.RemoveQueued(ctx, activeJobByID(t, jobs, retained.ID))
			if removalErr != nil || removed != statecontract.QueuedJobRemoved {
				t.Fatalf("transactional removal before rollback = %q, error = %v", removed, removalErr)
			}
			return wantErr
		})
	if !errors.Is(err, wantErr) {
		t.Fatalf("demand transaction rollback error = %v, want %v", err, wantErr)
	}
	requireQueued(t, store, retainedRequest, core.QueueAlreadyQueued)
	requireQueued(t, store, transientRequest, core.QueueQueued)
}
