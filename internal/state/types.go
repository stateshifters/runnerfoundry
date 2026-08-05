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
	HeartbeatMember(ctx context.Context, memberID core.MemberID, heartbeatAt time.Time) error
	ListLiveMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error)
	ListExpiredMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error)
	CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error)
	ExpireMember(ctx context.Context, memberID core.MemberID) error
}

type RunnerPoolCapacityStore interface {
	PublishRunnerPoolCapacity(ctx context.Context, memberID core.MemberID, runnerPool core.RunnerPool, capacity int) error
	AggregateRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error)
}
