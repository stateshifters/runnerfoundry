package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

type MembershipStore interface {
	HeartbeatMember(ctx context.Context, member core.MemberID, heartbeatAt time.Time) error
	PublishRunnerPoolCapacity(ctx context.Context, member core.MemberID, runnerPool core.RunnerPool, capacity int) error
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
	if err := service.store.HeartbeatMember(ctx, service.member, service.currentTime()); err != nil {
		return fmt.Errorf("heartbeat member %q: %w", service.member, err)
	}
	for _, capacity := range service.localCapacity {
		if err := service.store.PublishRunnerPoolCapacity(
			ctx, service.member, capacity.RunnerPool, capacity.Capacity,
		); err != nil {
			return fmt.Errorf("publish member %q Runner Pool %q capacity: %w", service.member, capacity.RunnerPool, err)
		}
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
		if _, err = service.failer.FailExpiredMember(ctx, member); err != nil {
			expirationErrors = append(expirationErrors, fmt.Errorf("fail attempts for expired member %q: %w", member, err))
			continue
		}
		if err = service.store.ExpireMember(ctx, member); err != nil {
			expirationErrors = append(expirationErrors, fmt.Errorf("expire member %q: %w", member, err))
			continue
		}
		expired++
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
