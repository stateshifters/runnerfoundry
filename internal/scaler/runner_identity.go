package scaler

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidRunnerIdentity reports an empty identity or a negative request ID.
	ErrInvalidRunnerIdentity = errors.New("invalid runner identity")
	// ErrRunnerIdentityNotFound reports that no eligible durable runner matched.
	ErrRunnerIdentityNotFound = errors.New("runner identity not found")
	// ErrRunnerIdentityMismatch reports that the fields of a pair do not identify one runner.
	ErrRunnerIdentityMismatch = errors.New("runner identity mismatch")
	// ErrRunnerIdentityInvariant reports that one identity selected multiple durable runners.
	ErrRunnerIdentityInvariant = errors.New("runner identity matched multiple runners")
)

// RunnerIdentity selects one durable runner by positive request ID, non-empty
// runner name, or both values. When both values are present, they identify the
// same row; implementations must not treat the fields as alternatives.
type RunnerIdentity struct {
	RequestID  int64
	RunnerName string
}

// Validate rejects identities that cannot select a durable runner.
func (identity RunnerIdentity) Validate() error {
	if identity.RequestID < 0 || (identity.RequestID == 0 && strings.TrimSpace(identity.RunnerName) == "") {
		return ErrInvalidRunnerIdentity
	}
	return nil
}

func (identity RunnerIdentity) hasRequestID() bool { return identity.RequestID > 0 }

func (identity RunnerIdentity) hasRunnerName() bool {
	return strings.TrimSpace(identity.RunnerName) != ""
}

func (identity RunnerIdentity) isPair() bool {
	return identity.hasRequestID() && identity.hasRunnerName()
}

// RunnerIdentityNotFoundError reports that no eligible runner matched an identity.
type RunnerIdentityNotFoundError struct {
	Identity RunnerIdentity
}

func (notFoundErr *RunnerIdentityNotFoundError) Error() string {
	return fmt.Sprintf("runner identity %+v: %v", notFoundErr.Identity, ErrRunnerIdentityNotFound)
}

func (notFoundErr *RunnerIdentityNotFoundError) Unwrap() error { return ErrRunnerIdentityNotFound }

// RunnerIdentityMismatchError reports that at least one field of a pair exists,
// but the complete pair does not identify the same eligible runner.
type RunnerIdentityMismatchError struct {
	Identity RunnerIdentity
}

func (mismatchErr *RunnerIdentityMismatchError) Error() string {
	return fmt.Sprintf("runner identity %+v: %v", mismatchErr.Identity, ErrRunnerIdentityMismatch)
}

func (mismatchErr *RunnerIdentityMismatchError) Unwrap() error { return ErrRunnerIdentityMismatch }

// RunnerIdentityInvariantError reports corrupt durable state that allowed one
// identity to select more than one runner.
type RunnerIdentityInvariantError struct {
	Identity RunnerIdentity
	Matches  int64
}

func (invariantErr *RunnerIdentityInvariantError) Error() string {
	return fmt.Sprintf("runner identity %+v matched %d runners: %v",
		invariantErr.Identity, invariantErr.Matches, ErrRunnerIdentityInvariant)
}

func (invariantErr *RunnerIdentityInvariantError) Unwrap() error { return ErrRunnerIdentityInvariant }
