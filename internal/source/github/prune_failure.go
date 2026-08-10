package github

import "errors"

// PruneFailure reports cleanup degradation after active registrations have
// already converged. Callers may safely activate the returned registrations.
type PruneFailure struct{ Err error }

func (failure PruneFailure) Error() string { return failure.Err.Error() }
func (failure PruneFailure) Unwrap() error { return failure.Err }

// IsPruneFailure reports whether reconciliation failed only during pruning.
func IsPruneFailure(err error) bool {
	var failure PruneFailure
	return errors.As(err, &failure)
}
