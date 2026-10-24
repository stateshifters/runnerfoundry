package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

type MembershipService struct {
	store         state.MemberRegistry
	member        core.MemberID
	localCapacity []core.RunnerPoolCapacity
	currentTime   func() time.Time
}

func NewMembershipService(
	store state.MemberRegistry,
	member core.MemberID,
	localCapacity []core.RunnerPoolCapacity,
	currentTime func() time.Time,
) *MembershipService {
	return &MembershipService{
		store: store, member: member,
		localCapacity: append([]core.RunnerPoolCapacity(nil), localCapacity...), currentTime: currentTime,
	}
}

func (service *MembershipService) Heartbeat(ctx context.Context) error {
	if err := service.store.PublishMemberState(ctx, service.member, service.currentTime(), service.localCapacity); err != nil {
		return fmt.Errorf("publish member %q state: %w", service.member, err)
	}
	return nil
}

// Withdraw removes this member's advertised capacity without deleting its
// durable identity or processing ownership.
func (service *MembershipService) Withdraw(ctx context.Context) error {
	if err := service.store.WithdrawMemberCapacity(ctx, service.member); err != nil {
		return fmt.Errorf("withdraw member %q capacity: %w", service.member, err)
	}
	return nil
}

func (service *MembershipService) CountLive(ctx context.Context, cutoff time.Time) (int, error) {
	count, err := service.store.CountLiveMembers(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("count members live at %s: %w", cutoff, err)
	}
	return count, nil
}
