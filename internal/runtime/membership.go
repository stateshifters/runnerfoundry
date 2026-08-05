package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

type MembershipStore interface {
	PublishMemberState(
		ctx context.Context,
		member core.MemberID,
		heartbeatAt time.Time,
		capacities []core.RunnerPoolCapacity,
	) error
	ListExpiredMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error)
	CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error)
	ExpireMember(ctx context.Context, member core.MemberID) error
}

type ExpiredMemberFailer interface {
	FailExpiredMember(ctx context.Context, member core.MemberID) (int, error)
}

// MembershipService coordinates explicit membership operations without owning
// timers. Capacity publication is separate from jobs except when expiration
// intentionally sends owned attempts through the failure-result path.
type MembershipService struct {
	store         MembershipStore
	failer        ExpiredMemberFailer
	member        core.MemberID
	localCapacity []core.RunnerPoolCapacity
	currentTime   func() time.Time
}

func NewMembershipService(
	store MembershipStore,
	failer ExpiredMemberFailer,
	member core.MemberID,
	localCapacity []core.RunnerPoolCapacity,
	currentTime func() time.Time,
) *MembershipService {
	return &MembershipService{
		store: store, failer: failer, member: member,
		localCapacity: append([]core.RunnerPoolCapacity(nil), localCapacity...), currentTime: currentTime,
	}
}

func (service *MembershipService) Heartbeat(ctx context.Context) error {
	if err := service.store.PublishMemberState(
		ctx, service.member, service.currentTime(), service.localCapacity,
	); err != nil {
		return fmt.Errorf("publish member %q state: %w", service.member, err)
	}
	return nil
}

func (service *MembershipService) Expire(ctx context.Context, cutoff time.Time) (int, error) {
	members, err := service.store.ListExpiredMembers(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("list members expired before %s: %w", cutoff, err)
	}

	expired := 0
	expirationErrors := make([]error, 0)
	for _, member := range members {
		attemptsTerminalized := true
		if _, err = service.failer.FailExpiredMember(ctx, member); err != nil {
			attemptsTerminalized = false
			expirationErrors = append(expirationErrors, fmt.Errorf("fail attempts for expired member %q: %w", member, err))
		}
		if err = service.store.ExpireMember(ctx, member); err != nil {
			expirationErrors = append(expirationErrors, fmt.Errorf("expire member %q: %w", member, err))
			continue
		}
		if attemptsTerminalized {
			expired++
		}
	}

	return expired, errors.Join(expirationErrors...)
}

func (service *MembershipService) CountLive(ctx context.Context, cutoff time.Time) (int, error) {
	count, err := service.store.CountLiveMembers(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("count members live at %s: %w", cutoff, err)
	}

	return count, nil
}
