package scaler

import (
	"errors"
	"fmt"
)

// ErrRunnerLifecycleConsistency identifies generic and compatibility lifecycle
// history that cannot be reconciled without overwriting immutable data.
var ErrRunnerLifecycleConsistency = errors.New("runner lifecycle representations are inconsistent")

// RunnerLifecycleConsistencyError identifies a generic/legacy lifecycle conflict
// that cannot be repaired without changing authoritative identity or provider history.
type RunnerLifecycleConsistencyError struct {
	ScaleSet   string
	Repository string
	RunnerName string
	Reason     string
}

func (err *RunnerLifecycleConsistencyError) Error() string {
	return fmt.Sprintf("runner lifecycle consistency for %s/%s/%s: %s",
		err.ScaleSet, err.Repository, err.RunnerName, err.Reason)
}

func (err *RunnerLifecycleConsistencyError) Unwrap() error {
	return ErrRunnerLifecycleConsistency
}
