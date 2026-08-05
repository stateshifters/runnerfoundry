package core

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

type MemberID = uuid.UUID

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
	Job      Job
	MemberID MemberID
}

type RunnerPoolCapacity struct {
	RunnerPool RunnerPool
	Capacity   int
}

// JobStore owns current queued and member-processed work. Taking work moves it
// atomically out of the FIFO queue; no completed-job history is retained here.
type JobStore interface {
	Enqueue(ctx context.Context, request JobRequest) (Job, bool, error)
	TakeOldest(ctx context.Context, runnerPool RunnerPool, member MemberID) (ProcessingJob, bool, error)
	CancelQueued(ctx context.Context, source SourceReference) error
	ProcessingByMember(ctx context.Context, member MemberID, runnerPool RunnerPool) ([]ProcessingJob, error)
	RemoveProcessing(ctx context.Context, jobID JobID) error
}
