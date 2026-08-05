package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestMembershipHeartbeatPublishesConfiguredLocalCapacity(t *testing.T) {
	t.Parallel()

	now := time.Unix(20, 0)
	events := make([]string, 0, 3)
	store := &membershipStoreFake{
		heartbeat: func(_ context.Context, member core.MemberID, at time.Time) error {
			events = append(events, "heartbeat:"+member.String())
			if !at.Equal(now) {
				t.Fatalf("heartbeat time = %s, want %s", at, now)
			}
			return nil
		},
		publish: func(_ context.Context, _ core.MemberID, pool core.RunnerPool, _ int) error {
			events = append(events, "capacity:"+string(pool))
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
	assertEvents(t, events, []string{"heartbeat:" + runtimeTestID(1).String(), "capacity:linux", "capacity:windows"})
}

func TestMembershipHeartbeatReportsRegistryAndCapacityFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("store unavailable")
	store := &membershipStoreFake{heartbeat: func(context.Context, core.MemberID, time.Time) error { return wantErr }}
	service := runtimeengine.NewMembershipService(store, nil, runtimeTestID(1), nil, fixedClock)
	if err := service.Heartbeat(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("heartbeat error = %v, want %v", err, wantErr)
	}

	store.heartbeat = func(context.Context, core.MemberID, time.Time) error { return nil }
	store.publish = func(context.Context, core.MemberID, core.RunnerPool, int) error { return wantErr }
	service = runtimeengine.NewMembershipService(store, nil, runtimeTestID(1), []core.RunnerPoolCapacity{{RunnerPool: "linux"}}, fixedClock)
	if err := service.Heartbeat(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("capacity error = %v, want %v", err, wantErr)
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
	heartbeat func(context.Context, core.MemberID, time.Time) error
	publish   func(context.Context, core.MemberID, core.RunnerPool, int) error
	expired   func(context.Context, time.Time) ([]core.MemberID, error)
	count     func(context.Context, time.Time) (int, error)
	expire    func(context.Context, core.MemberID) error
}

func (store *membershipStoreFake) HeartbeatMember(ctx context.Context, member core.MemberID, at time.Time) error {
	return store.heartbeat(ctx, member, at)
}
func (store *membershipStoreFake) PublishRunnerPoolCapacity(ctx context.Context, member core.MemberID, pool core.RunnerPool, capacity int) error {
	return store.publish(ctx, member, pool, capacity)
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
