package core

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

type MemberID string

type RunnerPool string

type RunnerName string

type SourceAdapter string

type SourceIdentifier string

type InventoryEndpointID string

type SourceReference struct {
	Adapter SourceAdapter
	ID      SourceIdentifier
}

type JobRequest struct {
	Source     SourceReference
	RunnerPool RunnerPool
	EnqueuedAt time.Time
}

type Job struct {
	ID         JobID
	Source     SourceReference
	RunnerPool RunnerPool
	EnqueuedAt time.Time
}

type ProcessingJob struct {
	Job                 Job
	MemberID            MemberID
	ClaimedAt           time.Time
	InventoryEndpointID InventoryEndpointID
	RunnerStarted       bool
	SourceTerminal      bool
}

type ProcessingClaim struct {
	MemberID            MemberID
	ClaimedAt           time.Time
	InventoryEndpointID InventoryEndpointID
}

type EnqueueResult struct {
	Job      Job
	Enqueued bool
}

type TakeResult struct {
	Processing ProcessingJob
	Taken      bool
}

type TransitionResult struct{ Applied bool }

type RunnerPoolCapacity struct {
	RunnerPool RunnerPool
	Capacity   int
}

// JobQueueStore owns source demand and source lifecycle observations. Queue is
// idempotent by source reference and preserves FIFO order by EnqueuedAt.
// TerminalizeSource and MarkRunnerStarted are transactional tombstone-guarded
// observations: either may arrive before, during, or after a local claim.
type JobQueueStore interface {
	Queue(ctx context.Context, request JobRequest) (EnqueueResult, error)
	TerminalizeSource(ctx context.Context, source SourceReference) error
	MarkRunnerStarted(ctx context.Context, source SourceReference) error
}

// ProcessingJobStore owns processing records and their guarded transitions.
// ClaimOldest serializes FIFO claims for a Runner Pool. Recovery and removal
// must compare the persisted claim state and return Applied=false when a
// concurrent source observation won; callers may safely retry errors.
type ProcessingClaimer interface {
	ClaimOldest(ctx context.Context, runnerPool RunnerPool, claim ProcessingClaim) (TakeResult, error)
}

type ProcessingReader interface {
	ProcessingByMember(ctx context.Context, member MemberID, runnerPool RunnerPool) ([]ProcessingJob, error)
	ProcessingByPool(ctx context.Context, runnerPool RunnerPool) ([]ProcessingJob, error)
}

type ProcessingTransitions interface {
	RetryUnstarted(ctx context.Context, jobID JobID) (TransitionResult, error)
	RecoverUnstartedProcessing(ctx context.Context, job ProcessingJob) (TransitionResult, error)
	RemoveTerminal(ctx context.Context, jobID JobID) (TransitionResult, error)
}

type ProcessingJobStore interface {
	ProcessingClaimer
	ProcessingReader
	ProcessingTransitions
}
