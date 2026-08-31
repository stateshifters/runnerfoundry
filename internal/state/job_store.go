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
}

// ProcessingClaimer atomically transfers the oldest eligible job in one
// Runner Pool from queued to member-owned processing state.
type ProcessingClaimer interface {
	ClaimOldest(
		ctx context.Context,
		runnerPool core.RunnerPool,
		claim core.ProcessingClaim,
	) (core.ClaimResult, error)
}

// ProcessingReader exposes durable processing snapshots used by member-local
// and leader-authorized reconciliation.
type ProcessingReader interface {
	ProcessingByMember(
		ctx context.Context,
		member core.MemberID,
		runnerPool core.RunnerPool,
	) ([]core.ProcessingJob, error)
	ProcessingByPool(ctx context.Context, runnerPool core.RunnerPool) ([]core.ProcessingJob, error)
}

// ProcessingTransitions owns guarded retry, recovery, and terminal removal.
// Implementations classify races transactionally and change ownership only for
// applied requeue or removal outcomes.
type ProcessingTransitions interface {
	// RetryUnstarted requeues only the claim identified by the supplied job,
	// claim time, inventory endpoint, and runner name. Replacement claims return
	// RetryClaimChanged without mutation.
	RetryUnstarted(ctx context.Context, processing core.ProcessingJob) (core.RetryResult, error)
	// RecoverUnstartedProcessing additionally guards the claim evidence supplied
	// by a reconciliation snapshot.
	RecoverUnstartedProcessing(ctx context.Context, job core.ProcessingJob) (core.RetryResult, error)
	// RemoveTerminal deletes only processing whose terminal observation is
	// already durable.
	RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TerminalRemovalResult, error)
}

type ProvisioningFailureTransitions interface {
	ReserveProvisioningFailure(
		ctx context.Context,
		processing core.ProcessingJob,
	) (core.RetryResult, error)
	RecordProvisioningFailureReported(
		ctx context.Context,
		processing core.ProcessingJob,
	) (core.RetryResult, error)
	FinalizeProvisioningFailure(
		ctx context.Context,
		processing core.ProcessingJob,
	) (core.TerminalRemovalResult, error)
}

// ProcessingJobStore is the complete source-neutral persistence boundary used
// by runtime orchestration and reconciliation.
type ProcessingJobStore interface {
	ProcessingClaimer
	ProcessingReader
	ProcessingTransitions
	ProvisioningFailureTransitions
}
