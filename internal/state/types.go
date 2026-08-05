package state

import (
	"context"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

type Registration struct {
	RepositoryURL  string
	RunnerPool     core.RunnerPool
	InstallationID int64
	RunnerGroupID  int64
	ScaleSetID     int64
}

type RegistrationStore interface {
	UpsertRegistration(ctx context.Context, registration Registration) error
	Registration(ctx context.Context, repositoryURL string, runnerPool core.RunnerPool) (Registration, bool, error)
	Registrations(ctx context.Context) ([]Registration, error)
	DeleteRegistration(ctx context.Context, repositoryURL string, runnerPool core.RunnerPool) error
}

type MemberRegistry interface {
	// PublishMemberState atomically renews the member lease and replaces its complete
	// Runner Pool capacity snapshot. Omitted and non-positive capacities are withdrawn.
	PublishMemberState(
		ctx context.Context,
		memberID core.MemberID,
		heartbeatAt time.Time,
		capacities []core.RunnerPoolCapacity,
	) error
	ListLiveMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error)
	ListExpiredMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error)
	CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error)
	ExpireMember(ctx context.Context, memberID core.MemberID) error
}

type RunnerPoolCapacityStore interface {
	// AvailableRunnerPoolCapacity returns live member capacity minus queued and attempting
	// jobs for the pool, clamped to zero. Result-pending jobs do not consume capacity.
	AvailableRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error)
}
