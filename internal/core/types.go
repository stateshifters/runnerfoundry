package core

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// JobID uniquely identifies durable queued and processing state.
type JobID = uuid.UUID

// MemberID identifies one active RunnerFoundry daemon member.
type MemberID string

// RunnerPool identifies a source-neutral pool of interchangeable runners.
type RunnerPool string

// RunnerName identifies one ephemeral runner across provider inventories.
type RunnerName string

// SourceAdapter identifies the source protocol that owns a work request.
type SourceAdapter string

// SourceIdentifier is the source protocol's stable work-request identifier.
type SourceIdentifier string

// InventoryEndpointID identifies the exact provider endpoint that accepted a
// processing claim and must be used for recovery.
type InventoryEndpointID string

// SourceReference is globally unique when Adapter and ID are considered
// together.
type SourceReference struct {
	Adapter SourceAdapter
	ID      SourceIdentifier
}

func (source SourceReference) IsEmpty() bool { return source.Adapter == "" && source.ID == "" }
func (source SourceReference) IsValid() bool { return source.Adapter != "" && source.ID != "" }

// RunnerObservation identifies a source lifecycle observation by the source's
// request reference, the runner name, or both when the source supplies both.
type RunnerObservation struct {
	Source     SourceReference
	RunnerName RunnerName
}

// JobIdentity selects durable work by source reference, RunnerFoundry job ID,
// or both. When both are present they must identify the same durable job.
type JobIdentity struct {
	Source SourceReference
	JobID  JobID
}

func (identity JobIdentity) Validate() error {
	if (!identity.Source.IsEmpty() && !identity.Source.IsValid()) ||
		(identity.Source.IsEmpty() && identity.JobID == uuid.Nil) {
		return ErrInvalidJobIdentity
	}
	return nil
}

var (
	// ErrInvalidJobIdentity means the selector contains neither a complete
	// source reference nor a job ID, or contains a partial source reference.
	ErrInvalidJobIdentity = errors.New("invalid job identity")
	// ErrJobIdentityMismatch means the supplied source and job ID identify
	// different durable jobs.
	ErrJobIdentityMismatch = errors.New("job identity source and job ID do not match")
)

// ObservationResult reports the durable effect of a lifecycle observation.
type ObservationResult struct{ Outcome ObservationOutcome }

// ObservationOutcome distinguishes a new transition from an idempotent
// replay and a valid observation whose durable job no longer exists.
type ObservationOutcome string

const (
	ObservationApplied        ObservationOutcome = "applied"
	ObservationAlreadyApplied ObservationOutcome = "already_applied"
	ObservationUnmatched      ObservationOutcome = "unmatched"
)

// JobRequest is source demand ready to enter a Runner Pool queue.
type JobRequest struct {
	Source     SourceReference
	RunnerPool RunnerPool
	EnqueuedAt time.Time
}

// Job is a durable queued work item with its RunnerFoundry-assigned identity.
type Job struct {
	ID         JobID
	Source     SourceReference
	RunnerPool RunnerPool
	EnqueuedAt time.Time
}

// ProcessingJob records exclusive member ownership of a claimed Job and the
// source observations needed for retry and terminal cleanup decisions.
type ProcessingJob struct {
	Job                 Job
	MemberID            MemberID
	ClaimedAt           time.Time
	InventoryEndpointID InventoryEndpointID
	RunnerStarted       bool
	SourceTerminal      bool
}

// ProcessingClaim supplies the member, time, and exact provider endpoint that
// become durable together when a queued Job is claimed.
type ProcessingClaim struct {
	MemberID            MemberID
	ClaimedAt           time.Time
	InventoryEndpointID InventoryEndpointID
}

// EnqueueResult reports both the durable Job, when one exists, and the exact
// idempotent queue outcome.
type EnqueueResult struct {
	Job     Job
	Outcome QueueOutcome
}

// QueueOutcome classifies an attempted queue transition without collapsing
// guarded no-ops into success.
type QueueOutcome string

const (
	QueueQueued         QueueOutcome = "queued"
	QueueAlreadyQueued  QueueOutcome = "already_queued"
	QueueProcessing     QueueOutcome = "processing"
	QueueSourceTerminal QueueOutcome = "source_terminal"
)

// ClaimResult contains a ProcessingJob only when Outcome is ClaimClaimed.
type ClaimResult struct {
	Processing ProcessingJob
	Outcome    ClaimOutcome
}

// ClaimOutcome distinguishes an acquired FIFO claim from an empty queue.
type ClaimOutcome string

const (
	ClaimClaimed ClaimOutcome = "claimed"
	ClaimEmpty   ClaimOutcome = "empty"
)

// RetryResult reports the exact guarded recovery transition.
type RetryResult struct{ Outcome RetryOutcome }

// RetryOutcome preserves whether recovery requeued work or observed a race
// that makes requeueing unsafe or unnecessary.
type RetryOutcome string

const (
	RetryRequeued       RetryOutcome = "requeued"
	RetryMissing        RetryOutcome = "missing"
	RetryRunnerStarted  RetryOutcome = "runner_started"
	RetrySourceTerminal RetryOutcome = "source_terminal"
	RetryClaimChanged   RetryOutcome = "claim_changed"
)

// TerminalRemovalResult reports the exact guarded terminal-removal transition.
type TerminalRemovalResult struct{ Outcome TerminalRemovalOutcome }

// TerminalRemovalOutcome distinguishes deletion from missing or non-terminal
// processing state.
type TerminalRemovalOutcome string

const (
	TerminalRemoved     TerminalRemovalOutcome = "removed"
	TerminalMissing     TerminalRemovalOutcome = "missing"
	TerminalNotTerminal TerminalRemovalOutcome = "not_terminal"
)

// RunnerPoolCapacity publishes one member's currently free local slots. A
// non-positive capacity withdraws that member's capacity for the Runner Pool.
type RunnerPoolCapacity struct {
	RunnerPool RunnerPool
	Capacity   int
}

// JobQueueStore owns source demand and source lifecycle observations. Queue is
// idempotent by source reference and preserves FIFO order by EnqueuedAt.
// Terminalize and MarkRunnerStarted are transactional tombstone-guarded
// observations: either may arrive before, during, or after a local claim.
type JobQueueStore interface {
	Queue(ctx context.Context, request JobRequest) (EnqueueResult, error)
	Terminalize(ctx context.Context, identity JobIdentity) (ObservationResult, error)
	MarkRunnerStarted(ctx context.Context, identity JobIdentity) (ObservationResult, error)
}

// ProcessingClaimer owns the atomic FIFO transition from queued to processing
// state for one Runner Pool. A claimed result durably records all claim fields.
type ProcessingClaimer interface {
	ClaimOldest(ctx context.Context, runnerPool RunnerPool, claim ProcessingClaim) (ClaimResult, error)
}

// ProcessingReader exposes processing ownership without changing it.
type ProcessingReader interface {
	ProcessingByMember(ctx context.Context, member MemberID, runnerPool RunnerPool) ([]ProcessingJob, error)
	ProcessingByPool(ctx context.Context, runnerPool RunnerPool) ([]ProcessingJob, error)
}

// ProcessingTransitions owns guarded retry, recovery, and terminal removal.
// Implementations classify races transactionally and change ownership only for
// applied requeue or removal outcomes.
type ProcessingTransitions interface {
	RetryUnstarted(ctx context.Context, jobID JobID) (RetryResult, error)
	RecoverUnstartedProcessing(ctx context.Context, job ProcessingJob) (RetryResult, error)
	RemoveTerminal(ctx context.Context, jobID JobID) (TerminalRemovalResult, error)
}

// ProcessingJobStore combines the processing claim, observation, and guarded
// transition contracts required by the runtime.
type ProcessingJobStore interface {
	ProcessingClaimer
	ProcessingReader
	ProcessingTransitions
}
