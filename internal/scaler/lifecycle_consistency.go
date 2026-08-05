package scaler

import (
	"errors"
	"fmt"
)

// ErrRunnerLifecycleConsistency identifies authoritative lifecycle state that
// conflicts with a requested identity or provider transition.
var ErrRunnerLifecycleConsistency = errors.New("runner lifecycle state is inconsistent")

// RunnerLifecycleConsistencyError identifies a transition that conflicts with
// authoritative identity or provider history.
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
