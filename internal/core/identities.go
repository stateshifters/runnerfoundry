package core

import (
	"errors"

	"github.com/google/uuid"
)

// JobID is assigned when source demand first enters durable state and remains
// stable across every provisioning attempt for that job. The zero value does
// not identify a durable job.
type JobID = uuid.UUID

// AttemptID distinguishes one provisioning claim from every other attempt for
// the same job. The zero value is invalid.
type AttemptID uuid.UUID

// MemberID identifies the runtime member that owns a processing claim and
// publishes local Runner Pool capacity. It is not a source identity.
type MemberID string

type RunnerPool string

// RunnerName carries either a source-assigned runner name or RunnerFoundry's
// encoded job and attempt identity. ParseRunnerName distinguishes the two.
type RunnerName string

// SourceAdapter is the stable namespace used to select one configured source
// implementation. Credentials and installations are not adapter namespaces.
type SourceAdapter string

// SourceIdentifier is an opaque adapter-owned identity whose uniqueness is
// scoped by SourceAdapter.
type SourceIdentifier string

// InventoryEndpointID identifies the exact execution endpoint that owns or
// can inspect a claimed runner.
type InventoryEndpointID string

// SourceReference is globally unique when Adapter and ID are considered
// together.
type SourceReference struct {
	Adapter SourceAdapter
	ID      SourceIdentifier
}

func (source SourceReference) IsEmpty() bool { return source.Adapter == "" && source.ID == "" }

// IsValid rejects partial source references; both fields are required whenever
// either field is present.
func (source SourceReference) IsValid() bool { return source.Adapter != "" && source.ID != "" }

// DemandScope is an opaque source-owned key that serializes demand mutations
// belonging to one upstream queue. It is not durable job identity.
type DemandScope struct {
	Adapter SourceAdapter
	ID      SourceIdentifier
}

func (scope DemandScope) IsValid() bool { return scope.Adapter != "" && scope.ID != "" }

// RunnerObservation carries whichever stable source reference and v2 runner
// name the source event supplies. When both identify a job, they must agree.
type RunnerObservation struct {
	Source     SourceReference
	RunnerName RunnerName
}

// JobIdentity selects durable work by source reference, RunnerFoundry job ID,
// or both. AttemptID narrows a job selector to one provisioning claim.
type JobIdentity struct {
	Source    SourceReference
	JobID     JobID
	AttemptID AttemptID
}

// Validate checks selector shape without reading durable state. Persistence
// implementations remain responsible for proving that two supplied identities
// select the same job.
func (identity JobIdentity) Validate() error {
	if (!identity.Source.IsEmpty() && !identity.Source.IsValid()) ||
		(identity.Source.IsEmpty() && identity.JobID == uuid.Nil) ||
		(identity.AttemptID.IsValid() && identity.JobID == uuid.Nil) {
		return ErrInvalidJobIdentity
	}
	return nil
}

var (
	// ErrInvalidJobIdentity means the selector contains neither a complete
	// source reference nor a job ID, or contains a partial source reference.
	ErrInvalidJobIdentity = errors.New("invalid job identity")
	// ErrJobIdentityMismatch means the supplied source and job ID identify
	// different durable jobs.
	ErrJobIdentityMismatch = errors.New("job identity source and job ID do not match")
)
