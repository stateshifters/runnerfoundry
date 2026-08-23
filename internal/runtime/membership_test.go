package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestMembershipPublishesCompleteStateAndCountsLiveMembers(t *testing.T) {
	memberID := core.MemberID("member")
	now := time.Unix(10, 0)
	capacities := []core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 3}}
	store := &membershipStoreFake{
		publish: func(_ context.Context, actualMember core.MemberID, heartbeat time.Time, actual []core.RunnerPoolCapacity) error {
			if actualMember != memberID || !heartbeat.Equal(now) || len(actual) != 1 || actual[0] != (core.RunnerPoolCapacity{RunnerPool: "linux", Capacity: 3}) {
				t.Fatalf("PublishMemberState(%s, %s, %#v)", actualMember, heartbeat, actual)
			}
			return nil
		},
		count: func(context.Context, time.Time) (int, error) { return 2, nil },
	}
	service := runtimeengine.NewMembershipService(store, memberID, capacities, func() time.Time { return now })
	capacities[0].Capacity = 99
	if err := service.Heartbeat(context.Background()); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	count, err := service.CountLive(context.Background(), now)
	if err != nil || count != 2 {
		t.Fatalf("CountLive() = (%d, %v)", count, err)
	}
}

func TestMembershipWithdrawsOnlyLocalCapacity(t *testing.T) {
	withdrawn := core.MemberID("")
	store := &membershipStoreFake{withdraw: func(_ context.Context, member core.MemberID) error {
		withdrawn = member
		return nil
	}}
	service := runtimeengine.NewMembershipService(store, "member", nil, func() time.Time { return time.Unix(2, 0) })
	if err := service.Withdraw(context.Background()); err != nil {
		t.Fatalf("Withdraw() error = %v", err)
	}
	if withdrawn != "member" {
		t.Fatalf("withdrawn member = %q", withdrawn)
	}
}

func TestMembershipReportsBoundaryFailures(t *testing.T) {
	wantErr := errors.New("state failed")
	store := &membershipStoreFake{
		publish:  func(context.Context, core.MemberID, time.Time, []core.RunnerPoolCapacity) error { return wantErr },
		count:    func(context.Context, time.Time) (int, error) { return 0, wantErr },
		withdraw: func(context.Context, core.MemberID) error { return wantErr },
	}
	service := runtimeengine.NewMembershipService(store, "member", nil, func() time.Time { return time.Unix(2, 0) })
	if err := service.Heartbeat(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	if err := service.Withdraw(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Withdraw() error = %v", err)
	}
	if _, err := service.CountLive(context.Background(), time.Time{}); !errors.Is(err, wantErr) {
		t.Fatalf("CountLive() error = %v", err)
	}
}

type membershipStoreFake struct {
	publish  func(context.Context, core.MemberID, time.Time, []core.RunnerPoolCapacity) error
	count    func(context.Context, time.Time) (int, error)
	withdraw func(context.Context, core.MemberID) error
}

func (store *membershipStoreFake) PublishMemberState(ctx context.Context, member core.MemberID, at time.Time, capacities []core.RunnerPoolCapacity) error {
	return store.publish(ctx, member, at, capacities)
}
func (store *membershipStoreFake) CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error) {
	return store.count(ctx, cutoff)
}
func (store *membershipStoreFake) WithdrawMemberCapacity(ctx context.Context, member core.MemberID) error {
	return store.withdraw(ctx, member)
}
