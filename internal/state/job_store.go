package state

import (
	"context"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

// JobQueueStore owns source demand and source lifecycle observations. Queue is
// idempotent by source reference and preserves FIFO order by EnqueuedAt.
type JobQueueStore interface {
	// Queue returns explicit replay and conflict outcomes for one stable source
	// reference; nil error does not imply that a new job was inserted.
	Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error)
	// Terminalize records a terminal tombstone before removing queued demand so
	// delayed or replayed observations cannot recreate it.
	Terminalize(ctx context.Context, identity core.JobIdentity) (core.ObservationResult, error)
	// ObserveRunnerAcceptance records the source-observed no-retry boundary and
	// resolves an acceptance that races ahead of the local claim.
	ObserveRunnerAcceptance(ctx context.Context, identity core.JobIdentity) (core.ObservationResult, error)
	ObserveTerminalIfUnaccepted(ctx context.Context, identity core.JobIdentity) (core.ConditionalTerminalResult, error)
}

type ProcessingJobStore interface {
	ClaimOldest(ctx context.Context, runnerPool core.RunnerPool, claim core.ProcessingClaim) (core.ClaimResult, error)
	ProcessingByMember(ctx context.Context, member core.MemberID, runnerPool core.RunnerPool) ([]core.ProcessingJob, error)
	ProcessingByPool(ctx context.Context, runnerPool core.RunnerPool) ([]core.ProcessingJob, error)
}

type ProcessingTransitions interface {
	RetryUnstarted(ctx context.Context, processing core.ProcessingJob) (core.RetryResult, error)
	RecoverUnstartedProcessing(ctx context.Context, processing core.ProcessingJob) (core.RetryResult, error)
	RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TerminalRemovalResult, error)
}

type ProvisioningFailureTransitions interface {
	ReserveProvisioningFailure(ctx context.Context, processing core.ProcessingJob) (core.RetryResult, error)
	RecordProvisioningFailureReported(ctx context.Context, processing core.ProcessingJob) (core.RetryResult, error)
	FinalizeProvisioningFailure(ctx context.Context, processing core.ProcessingJob) (core.TerminalRemovalResult, error)
}
