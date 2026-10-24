package core

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestSourceReferenceClassifiesCompleteAndPartialIdentities(t *testing.T) {
	for name, testCase := range map[string]struct {
		reference SourceReference
		empty     bool
		valid     bool
	}{
		"empty":        {reference: SourceReference{}, empty: true},
		"complete":     {reference: SourceReference{Adapter: "test-source", ID: "request"}, valid: true},
		"adapter only": {reference: SourceReference{Adapter: "test-source"}},
		"ID only":      {reference: SourceReference{ID: "request"}},
	} {
		t.Run(name, func(t *testing.T) {
			if testCase.reference.IsEmpty() != testCase.empty || testCase.reference.IsValid() != testCase.valid {
				t.Fatalf("SourceReference classification = (empty %t, valid %t)",
					testCase.reference.IsEmpty(), testCase.reference.IsValid())
			}
		})
	}
}

func TestDemandScopeRequiresCompleteSourceOwnedIdentity(t *testing.T) {
	for name, testCase := range map[string]struct {
		scope DemandScope
		valid bool
	}{
		"complete":     {scope: DemandScope{Adapter: "test-source", ID: "repository-pool"}, valid: true},
		"empty":        {},
		"adapter only": {scope: DemandScope{Adapter: "test-source"}},
		"ID only":      {scope: DemandScope{ID: "repository-pool"}},
	} {
		t.Run(name, func(t *testing.T) {
			if testCase.scope.IsValid() != testCase.valid {
				t.Fatalf("IsValid() = %t, want %t", testCase.scope.IsValid(), testCase.valid)
			}
		})
	}
}

func TestJobIdentityAcceptsEveryCompleteSelectorForm(t *testing.T) {
	source := SourceReference{Adapter: "test-source", ID: "request"}
	jobID := uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764ffe")
	attemptID := AttemptID(uuid.MustParse("028fbb38-3d4e-7a58-a608-f23e1b764ffe"))
	for name, identity := range map[string]JobIdentity{
		"source":                     {Source: source},
		"job ID":                     {JobID: jobID},
		"source and job ID":          {Source: source, JobID: jobID},
		"job and attempt ID":         {JobID: jobID, AttemptID: attemptID},
		"source, job and attempt ID": {Source: source, JobID: jobID, AttemptID: attemptID},
	} {
		t.Run(name, func(t *testing.T) {
			if err := identity.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestJobIdentityRejectsMissingAndPartialSelectors(t *testing.T) {
	for name, identity := range map[string]JobIdentity{
		"missing":         {},
		"adapter only":    {Source: SourceReference{Adapter: "test-source"}},
		"ID only":         {Source: SourceReference{ID: "request"}},
		"attempt ID only": {AttemptID: AttemptID{1}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := identity.Validate(); !errors.Is(err, ErrInvalidJobIdentity) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}
