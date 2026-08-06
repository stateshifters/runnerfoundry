package core

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

type MemberID string

type RunnerPool string

type SourceReference struct {
	Adapter string
	ID      string
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
	Job            Job
	MemberID       MemberID
	RunnerStarted  bool
	SourceTerminal bool
}

type RunnerPoolCapacity struct {
	RunnerPool RunnerPool
	Capacity   int
}

// JobQueueStore owns source demand and source lifecycle observations.
type JobQueueStore interface {
	Enqueue(ctx context.Context, request JobRequest) (Job, bool, error)
	TerminalizeSource(ctx context.Context, source SourceReference) error
	MarkRunnerStarted(ctx context.Context, source SourceReference) error
}

// ProcessingJobStore owns member processing records and their guarded transitions.
type ProcessingJobStore interface {
	TakeOldest(ctx context.Context, runnerPool RunnerPool, member MemberID) (ProcessingJob, bool, error)
	ProcessingByMember(ctx context.Context, member MemberID, runnerPool RunnerPool) ([]ProcessingJob, error)
	RetryUnstartedProcessing(ctx context.Context, jobID JobID) (bool, error)
	RemoveTerminalProcessing(ctx context.Context, jobID JobID) (bool, error)
}
