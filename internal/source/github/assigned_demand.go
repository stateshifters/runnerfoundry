package github

import (
	"context"
	"errors"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

// AssignedDemand is GitHub's authoritative desired active-job count for one
// repository-scoped Runner Pool.
type AssignedDemand struct {
	Repository    string
	RunnerPool    core.RunnerPool
	ObservationID string
	DesiredJobs   int
	ObservedAt    time.Time
}

// AssignedDemandResult describes the ordinary durable jobs covering one
// assigned-demand observation after its transaction commits.
type AssignedDemandResult struct {
	ActiveJobs     int
	QueuedJobs     int
	ProcessingJobs int
	JobsAdded      int
	JobsRemoved    int
}

// AssignedDemandRepository atomically translates GitHub's aggregate demand
// into ordinary queued jobs without creating a second scheduler.
type AssignedDemandRepository interface {
	ReconcileAssignedDemand(ctx context.Context, demand AssignedDemand) (AssignedDemandResult, error)
}

func validateAssignedDemand(demand AssignedDemand) error {
	if demand.Repository == "" || demand.RunnerPool == "" || demand.ObservationID == "" ||
		demand.DesiredJobs < 0 || demand.ObservedAt.IsZero() {
		return errors.New("complete non-negative GitHub assigned demand is required")
	}
	return nil
}

// Validate rejects incomplete or impossible upstream demand before a state
// backend starts a transaction.
func (demand AssignedDemand) Validate() error { return validateAssignedDemand(demand) }
