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

type JobState string

const (
	JobStateQueued        JobState = "queued"
	JobStateAttempting    JobState = "attempting"
	JobStateResultPending JobState = "result_pending"
)

type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
)

type ResultOrigin string

const (
	ResultOriginRunner  ResultOrigin = "runner"
	ResultOriginService ResultOrigin = "service"
)

type Result struct {
	Outcome Outcome
	Origin  ResultOrigin
	Reason  string
}

type JobRecord struct {
	Job                  Job
	State                JobState
	AttemptingMember     MemberID
	ClaimedAt            time.Time
	StartedAt            time.Time
	AcquisitionStartedAt time.Time
	AcquiredAt           time.Time
	Result               *Result
}

type ResultDelivery string

const (
	ResultDeliveryReported      ResultDelivery = "reported"
	ResultDeliveryRunnerManaged ResultDelivery = "runner_managed"
	ResultDeliveryUnsupported   ResultDelivery = "unsupported"
)

type Receipt struct {
	Source   SourceReference
	Outcome  Outcome
	Origin   ResultOrigin
	Delivery ResultDelivery
}

type RunnerPoolCapacity struct {
	RunnerPool RunnerPool
	Capacity   int
}

type JobQueueStore interface {
	Enqueue(ctx context.Context, request JobRequest) (JobRecord, bool, error)
	ClaimOldest(ctx context.Context, runnerPool RunnerPool, member MemberID, claimedAt time.Time) (JobRecord, bool, error)
}

type JobAttemptStore interface {
	MarkStartedBySource(ctx context.Context, source SourceReference, startedAt time.Time) (JobRecord, error)
	BeginAcquisition(ctx context.Context, jobID JobID, at time.Time) (JobRecord, error)
	ConfirmAcquisition(ctx context.Context, jobID JobID, at time.Time) (JobRecord, error)
	AttemptsByMember(ctx context.Context, member MemberID, runnerPool RunnerPool) ([]JobRecord, error)
	PendingAcquisitions(ctx context.Context, limit int) ([]JobRecord, error)
}

type JobResultStore interface {
	SetResultPending(ctx context.Context, jobID JobID, result Result) (JobRecord, error)
	SetResultPendingBySource(ctx context.Context, source SourceReference, result Result) (JobRecord, error)
	Finalize(ctx context.Context, jobID JobID, delivery ResultDelivery) (Receipt, error)
	LookupReceipt(ctx context.Context, source SourceReference) (Receipt, bool, error)
	PendingResults(ctx context.Context, limit int) ([]JobRecord, error)
}

type JobFailureStore interface {
	SetMemberAttemptsResultPending(ctx context.Context, member MemberID, result Result) ([]JobRecord, error)
	SetUnstartedAttemptsResultPending(ctx context.Context, runnerPool RunnerPool, before time.Time, result Result) ([]JobRecord, error)
	Lookup(ctx context.Context, jobID JobID) (JobRecord, bool, error)
}

// JobStore owns the one durable job lifecycle shared by every backend.
type JobStore interface {
	JobQueueStore
	JobAttemptStore
	JobResultStore
	JobFailureStore
}

type ResultReporter interface {
	ReportResult(ctx context.Context, job Job, result Result) (ResultDelivery, error)
}
