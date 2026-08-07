package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestExclusiveLeadershipRetainsOneLiveLease(t *testing.T) {
	t.Parallel()

	lease := &leaderLeaseFake{alive: true}
	attempts := 0
	leadership := newExclusiveLeadership(errors.New("not leader"), func(context.Context) (leaderLease, error) {
		attempts++
		return lease, nil
	})

	for call := 0; call < 2; call++ {
		leader, err := leadership.IsLeader(context.Background())
		if err != nil || !leader {
			t.Fatalf("leadership call %d = %t, %v", call, leader, err)
		}
	}
	if attempts != 1 {
		t.Fatalf("lease attempts = %d, want 1", attempts)
	}
	if err := leadership.Close(context.Background()); err != nil {
		t.Fatalf("close leadership: %v", err)
	}
	if lease.closes != 1 {
		t.Fatalf("lease closes = %d, want 1", lease.closes)
	}
}

func TestExclusiveLeadershipReplacesDeadLease(t *testing.T) {
	t.Parallel()

	dead := &leaderLeaseFake{}
	replacement := &leaderLeaseFake{alive: true}
	leases := []leaderLease{dead, replacement}
	leadership := newExclusiveLeadership(errors.New("not leader"), func(context.Context) (leaderLease, error) {
		lease := leases[0]
		leases = leases[1:]
		return lease, nil
	})
	if leader, err := leadership.IsLeader(context.Background()); err != nil || !leader {
		t.Fatalf("first leadership = %t, %v", leader, err)
	}
	if leader, err := leadership.IsLeader(context.Background()); err != nil || !leader {
		t.Fatalf("replacement leadership = %t, %v", leader, err)
	}
	if dead.closes != 1 {
		t.Fatalf("dead lease closes = %d, want 1", dead.closes)
	}
}

func TestExclusiveLeadershipTreatsContendedLeaseAsFollower(t *testing.T) {
	t.Parallel()

	notLeader := errors.New("not leader")
	leadership := newExclusiveLeadership(notLeader, func(context.Context) (leaderLease, error) {
		return nil, notLeader
	})
	leader, err := leadership.IsLeader(context.Background())
	if err != nil || leader {
		t.Fatalf("leadership = %t, %v, want follower", leader, err)
	}
}

func TestExclusiveLeadershipReportsLeaseFailuresAndIdempotentClose(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("lease failed")
	leadership := newExclusiveLeadership(errors.New("not leader"), func(context.Context) (leaderLease, error) {
		return nil, wantErr
	})
	if _, err := leadership.IsLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("acquire error = %v", err)
	}
	if err := leadership.Close(context.Background()); err != nil {
		t.Fatalf("close empty leadership: %v", err)
	}
	lease := &leaderLeaseFake{closeErr: wantErr}
	leadership.lease = lease
	if _, err := leadership.IsLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("lost lease close error = %v", err)
	}
	lease.alive = true
	leadership.lease = lease
	if err := leadership.Close(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close leader error = %v", err)
	}
}

func TestLocalLeadershipHandsSQLiteLeaderToReplacementAfterOldGenerationCloses(t *testing.T) {
	t.Parallel()

	gate := &localLeadershipGate{}
	active := gate.NewLeadership()
	replacement := gate.NewLeadership()
	assertLocalFollower(t, active, "unactivated")
	assertNoTestError(t, active.Activate(), "activate initial leadership")
	assertLocalLeader(t, active, "activated")
	assertLocalFollower(t, replacement, "replacement overlap")
	if err := replacement.Activate(); err == nil {
		t.Fatal("replacement activated while initial generation owned leadership")
	}
	assertNoTestError(t, active.Close(context.Background()), "close active leadership")
	assertLocalFollower(t, replacement, "replacement before activation")
	assertNoTestError(t, replacement.Activate(), "activate replacement leadership")
	assertLocalLeader(t, replacement, "replacement")
}

func assertLocalLeader(t *testing.T, leadership *localLeadership, phase string) {
	t.Helper()
	leader, err := leadership.IsLeader(context.Background())
	if err != nil || !leader {
		t.Fatalf("%s leadership = %t, %v, want leader", phase, leader, err)
	}
}

func assertLocalFollower(t *testing.T, leadership *localLeadership, phase string) {
	t.Helper()
	leader, err := leadership.IsLeader(context.Background())
	if err != nil || leader {
		t.Fatalf("%s leadership = %t, %v, want follower", phase, leader, err)
	}
}

func assertNoTestError(t *testing.T, err error, operation string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

func assertTestError(t *testing.T, err error, operation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s unexpectedly succeeded", operation)
	}
}

type leaderLeaseFake struct {
	alive    bool
	closes   int
	closeErr error
}

func (lease *leaderLeaseFake) Alive(context.Context) bool { return lease.alive }
func (lease *leaderLeaseFake) Close(context.Context) error {
	lease.closes++
	return lease.closeErr
}
