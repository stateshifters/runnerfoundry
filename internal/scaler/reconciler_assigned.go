package scaler

import (
	"context"
	"errors"
)

// AssignedRunnerPresent reports whether the identity-free runner for the
// configured durable assignment is visible through the provider. It is only
// valid when AssignmentID is positive.
func (reconciler Reconciler) AssignedRunnerPresent(ctx context.Context) (bool, error) {
	if reconciler.ScaleSet.AssignmentID <= 0 {
		return false, errors.New("durable assignment ID is required")
	}
	return reconciler.RunnerPresent(
		ctx,
		runnerName(reconciler.ScaleSet.Name, reconciler.ScaleSet.AssignmentID),
	)
}

func runningRunnerExists(instances []RunnerInstance, expectedName string) bool {
	for _, instance := range instances {
		if instance.RunnerName == expectedName && instance.State == RunnerStateRunning {
			return true
		}
	}
	return false
}
