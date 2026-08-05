package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestMembershipHeartbeatPublishesConfiguredLocalCapacity(t *testing.T) {
	t.Parallel()

	now := time.Unix(20, 0)
	events := make([]string, 0, 1)
	store := &membershipStoreFake{
		publish: func(_ context.Context, member core.MemberID, at time.Time, capacities []core.RunnerPoolCapacity) error {
			events = append(events, "member-state:"+member.String())
			if !at.Equal(now) {
				t.Fatalf("heartbeat time = %s, want %s", at, now)
			}
			want := []core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}, {RunnerPool: "windows", Capacity: 1}}
			if !reflect.DeepEqual(capacities, want) {
				t.Fatalf("capacities = %#v, want %#v", capacities, want)
			}
			return nil
		},
	}
	service := runtimeengine.NewMembershipService(
		store, nil, runtimeTestID(1),
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}, {RunnerPool: "windows", Capacity: 1}},
		func() time.Time { return now },
	)

	if err := service.Heartbeat(context.Background()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	assertEvents(t, events, []string{"member-state:" + runtimeTestID(1).String()})
}

func TestMembershipHeartbeatReportsAtomicPublicationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("store unavailable")
	store := &membershipStoreFake{publish: func(context.Context, core.MemberID, time.Time, []core.RunnerPoolCapacity) error {
		return wantErr
	}}
	service := runtimeengine.NewMembershipService(store, nil, runtimeTestID(1), nil, fixedClock)
	if err := service.Heartbeat(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("heartbeat error = %v, want %v", err, wantErr)
	}
}

func TestMembershipExpiryFailsAttemptsBeforeExpiringIdentity(t *testing.T) {
	t.Parallel()

	events := make([]string, 0, 4)
	store := &membershipStoreFake{
		expired: func(context.Context, time.Time) ([]core.MemberID, error) {
			return []core.MemberID{runtimeTestID(10), runtimeTestID(11)}, nil
		},
		expire: func(_ context.Context, member core.MemberID) error {
			events = append(events, "expire:"+member.String())
			return nil
		},
	}
	failer := expiredMemberFailerFake(func(_ context.Context, member core.MemberID) (int, error) {
		events = append(events, "fail:"+member.String())
		return 1, nil
	})
	service := runtimeengine.NewMembershipService(store, failer, runtimeTestID(1), nil, fixedClock)

	expired, err := service.Expire(context.Background(), time.Unix(10, 0))
	if err != nil || expired != 2 {
		t.Fatalf("expire members = (%d, %v), want (2, nil)", expired, err)
	}
	assertEvents(t, events, []string{
		"fail:" + runtimeTestID(10).String(), "expire:" + runtimeTestID(10).String(),
		"fail:" + runtimeTestID(11).String(), "expire:" + runtimeTestID(11).String(),
	})
}

func TestMembershipCountLiveDelegatesToRegistry(t *testing.T) {
	t.Parallel()

	store := &membershipStoreFake{count: func(context.Context, time.Time) (int, error) { return 3, nil }}
	service := runtimeengine.NewMembershipService(store, nil, runtimeTestID(1), nil, fixedClock)
	count, err := service.CountLive(context.Background(), time.Unix(10, 0))
	if err != nil || count != 3 {
		t.Fatalf("live members = (%d, %v), want (3, nil)", count, err)
	}
}

func TestMembershipExpiryReportsEachFailureBoundary(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("expiry unavailable")
	store := &membershipStoreFake{expired: func(context.Context, time.Time) ([]core.MemberID, error) { return nil, wantErr }}
	service := runtimeengine.NewMembershipService(store, nil, runtimeTestID(1), nil, fixedClock)
	if _, err := service.Expire(context.Background(), time.Unix(1, 0)); !errors.Is(err, wantErr) {
		t.Fatalf("list expired error = %v, want %v", err, wantErr)
	}

	store.expired = func(context.Context, time.Time) ([]core.MemberID, error) {
		return []core.MemberID{runtimeTestID(10), runtimeTestID(11)}, nil
	}
	store.expire = func(_ context.Context, member core.MemberID) error {
		if member == runtimeTestID(11) {
			return wantErr
		}
		return nil
	}
	failer := expiredMemberFailerFake(func(_ context.Context, member core.MemberID) (int, error) {
		if member == runtimeTestID(10) {
			return 0, wantErr
		}
		return 0, nil
	})
	service = runtimeengine.NewMembershipService(store, failer, runtimeTestID(1), nil, fixedClock)
	if expired, err := service.Expire(context.Background(), time.Unix(1, 0)); expired != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("member expiry failures = (%d, %v), want joined failures", expired, err)
	}
}

func TestMembershipExpiryWithdrawsCapacityWhenFailingAttemptsFails(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("attempt cleanup unavailable")
	member := runtimeTestID(10)
	withdrawn := false
	store := &membershipStoreFake{
		expired: func(context.Context, time.Time) ([]core.MemberID, error) { return []core.MemberID{member}, nil },
		expire: func(_ context.Context, expiredMember core.MemberID) error {
			withdrawn = expiredMember == member
			return nil
		},
	}
	failer := expiredMemberFailerFake(func(context.Context, core.MemberID) (int, error) { return 0, wantErr })
	service := runtimeengine.NewMembershipService(store, failer, runtimeTestID(1), nil, fixedClock)

	expired, err := service.Expire(context.Background(), time.Unix(1, 0))
	if expired != 0 || !errors.Is(err, wantErr) || !withdrawn {
		t.Fatalf("expiry = (%d, %v), withdrawn=%t, want failure with capacity withdrawn", expired, err, withdrawn)
	}
}

func TestMembershipCountLiveReportsRegistryFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("count unavailable")
	store := &membershipStoreFake{count: func(context.Context, time.Time) (int, error) { return 0, wantErr }}
	service := runtimeengine.NewMembershipService(store, nil, runtimeTestID(1), nil, fixedClock)
	if _, err := service.CountLive(context.Background(), time.Unix(1, 0)); !errors.Is(err, wantErr) {
		t.Fatalf("count error = %v, want %v", err, wantErr)
	}
}

type membershipStoreFake struct {
	publish func(context.Context, core.MemberID, time.Time, []core.RunnerPoolCapacity) error
	expired func(context.Context, time.Time) ([]core.MemberID, error)
	count   func(context.Context, time.Time) (int, error)
	expire  func(context.Context, core.MemberID) error
}

func (store *membershipStoreFake) PublishMemberState(
	ctx context.Context,
	member core.MemberID,
	at time.Time,
	capacities []core.RunnerPoolCapacity,
) error {
	return store.publish(ctx, member, at, capacities)
}
func (store *membershipStoreFake) ListExpiredMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error) {
	return store.expired(ctx, cutoff)
}
func (store *membershipStoreFake) CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error) {
	return store.count(ctx, cutoff)
}
func (store *membershipStoreFake) ExpireMember(ctx context.Context, member core.MemberID) error {
	return store.expire(ctx, member)
}

type expiredMemberFailerFake func(context.Context, core.MemberID) (int, error)

func (failer expiredMemberFailerFake) FailExpiredMember(ctx context.Context, member core.MemberID) (int, error) {
	return failer(ctx, member)
}
