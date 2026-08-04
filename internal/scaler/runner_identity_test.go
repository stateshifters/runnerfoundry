package scaler

import (
	"errors"
	"testing"
)

func TestRunnerIdentityValidation(t *testing.T) {
	tests := []struct {
		name     string
		identity RunnerIdentity
		wantErr  bool
	}{
		{name: "request ID", identity: RunnerIdentity{RequestID: 42}},
		{name: "runner name", identity: RunnerIdentity{RunnerName: "linux-42"}},
		{name: "pair", identity: RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}},
		{name: "empty", identity: RunnerIdentity{}, wantErr: true},
		{name: "negative request ID", identity: RunnerIdentity{RequestID: -1}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.identity.Validate()
			if test.wantErr && !errors.Is(err, ErrInvalidRunnerIdentity) {
				t.Fatalf("Validate() error = %v, want ErrInvalidRunnerIdentity", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestRunnerIdentityErrorsExposeTypedDetails(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}
	tests := []struct {
		name     string
		err      error
		sentinel error
		as       func(error) bool
	}{
		{name: "not found", err: &RunnerIdentityNotFoundError{Identity: identity}, sentinel: ErrRunnerIdentityNotFound,
			as: func(err error) bool { var target *RunnerIdentityNotFoundError; return errors.As(err, &target) }},
		{name: "mismatch", err: &RunnerIdentityMismatchError{Identity: identity}, sentinel: ErrRunnerIdentityMismatch,
			as: func(err error) bool { var target *RunnerIdentityMismatchError; return errors.As(err, &target) }},
		{name: "invariant", err: &RunnerIdentityInvariantError{Identity: identity, Matches: 2}, sentinel: ErrRunnerIdentityInvariant,
			as: func(err error) bool { var target *RunnerIdentityInvariantError; return errors.As(err, &target) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertTypedRunnerIdentityError(t, test.err, test.sentinel, test.as)
		})
	}
}

func assertTypedRunnerIdentityError(t *testing.T, err error, sentinel error, matchesType func(error) bool) {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is(%v, %v) = false", err, sentinel)
	}
	if err.Error() == "" {
		t.Fatal("typed identity error has empty text")
	}
	if !matchesType(err) {
		t.Fatalf("errors.As(%T) = false", err)
	}
}
