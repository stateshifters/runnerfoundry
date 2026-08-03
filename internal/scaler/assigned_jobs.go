package scaler

import (
	"context"
	"fmt"
	"sync"
)

// AssignedJobBackend adapts one repository and scale set to assigned-job reconciliation.
// Prepare must observe the authoritative count, apply backend-specific cleanup before returning,
// and return the capacity available to that repository. StartAssigned may return false when
// capacity changed after preparation; the controller then stops without treating the race as an error.
type AssignedJobBackend interface {
	Prepare(ctx context.Context, assignedJobs int) (AssignedJobCapacity, error)
	StartAssigned(ctx context.Context) (bool, error)
}

// AssignedJobCapacity describes current runner usage after backend-specific observation and cleanup.
type AssignedJobCapacity struct {
	ActiveRunners     int
	AvailableCapacity int
}

// AssignedReconciliation summarizes one authoritative assigned-job observation.
type AssignedReconciliation struct {
	AssignedJobs   int
	RunningRunners int
	StartedRunners int
}

// AssignedJobController serializes assigned-job observations for one repository and scale set.
// A controller must not be shared by different repository or scale-set backends.
type AssignedJobController struct {
	backend AssignedJobBackend
	mutex   sync.Mutex
}

// NewAssignedJobController binds assigned-job reconciliation to one backend capability.
func NewAssignedJobController(backend AssignedJobBackend) *AssignedJobController {
	return &AssignedJobController{backend: backend}
}

// Reconcile replaces the authoritative assigned count and starts the missing runners that
// fit within the capacity returned by the backend. Concurrent calls execute one at a time.
func (controller *AssignedJobController) Reconcile(
	ctx context.Context,
	assignedJobs int,
) (AssignedReconciliation, error) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	capacity, err := controller.backend.Prepare(ctx, assignedJobs)
	if err != nil {
		return AssignedReconciliation{}, fmt.Errorf("prepare assigned jobs: %w", err)
	}
	result := AssignedReconciliation{AssignedJobs: assignedJobs, RunningRunners: capacity.ActiveRunners}
	deficit := assignedJobs - capacity.ActiveRunners
	for deficit > 0 && capacity.AvailableCapacity > 0 {
		started, startErr := controller.backend.StartAssigned(ctx)
		if startErr != nil {
			return AssignedReconciliation{}, fmt.Errorf("start assigned runner: %w", startErr)
		}
		if !started {
			break
		}
		deficit--
		capacity.AvailableCapacity--
		result.StartedRunners++
	}
	return result, nil
}
