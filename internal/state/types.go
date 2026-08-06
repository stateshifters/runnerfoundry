package state

import (
	"context"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

type Registration = githubsource.Registration

type RegistrationStore interface {
	githubsource.RegistrationRepository
	UpsertRegistration(ctx context.Context, registration Registration) error
	Registration(ctx context.Context, repositoryURL string, runnerPool core.RunnerPool) (Registration, bool, error)
	Registrations(ctx context.Context) ([]Registration, error)
	DeleteRegistration(ctx context.Context, repositoryURL string, runnerPool core.RunnerPool) error
}

type MemberRegistry interface {
	// PublishMemberState atomically renews the member heartbeat and replaces its complete
	// Runner Pool capacity snapshot. Omitted and non-positive capacities are withdrawn.
	PublishMemberState(
		ctx context.Context,
		memberID core.MemberID,
		heartbeatAt time.Time,
		capacities []core.RunnerPoolCapacity,
	) error
	CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error)
	WithdrawMemberCapacity(ctx context.Context, memberID core.MemberID) error
}

type RunnerPoolCapacityStore interface {
	// AvailableRunnerPoolCapacity returns live member capacity minus queued jobs and
	// every processing job, clamped to zero. Processing remains capacity-neutral
	// while proof-based recovery moves it back to the queue.
	AvailableRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error)
}
