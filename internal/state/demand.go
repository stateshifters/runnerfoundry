package state

import (
	"context"
	"errors"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

type ActiveJobPhase string

const (
	ActiveJobQueued     ActiveJobPhase = "queued"
	ActiveJobProcessing ActiveJobPhase = "processing"
)

type ActiveJob struct {
	ID             core.JobID
	Source         core.SourceReference
	RunnerPool     core.RunnerPool
	Phase          ActiveJobPhase
	StateChangedAt time.Time
}

type QueuedRemovalOutcome string

const (
	QueuedJobRemoved QueuedRemovalOutcome = "removed"
	QueuedJobMissing QueuedRemovalOutcome = "missing"
)

type DemandTransaction interface {
	ActiveJobs(ctx context.Context) ([]ActiveJob, error)
	Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error)
	RemoveQueued(ctx context.Context, job ActiveJob) (QueuedRemovalOutcome, error)
}

type DemandTransactionStore interface {
	WithDemandTransaction(ctx context.Context, key DemandTransactionKey, reconcile func(DemandTransaction) error) error
}

type DemandTransactionKey struct {
	Scope      core.DemandScope
	RunnerPool core.RunnerPool
}

var ErrDemandTransactionMismatch = errors.New("demand transaction scope mismatch")

type MemberLivenessReader interface {
	LiveMemberIDs(ctx context.Context, cutoff time.Time) ([]core.MemberID, error)
}
