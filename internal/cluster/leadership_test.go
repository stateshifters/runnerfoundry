package cluster

import (
	"context"
	"errors"
	"testing"
)

func TestLeaderElectorReturnsSessionBackedLease(t *testing.T) {
	connections := connectionsForTest(t, Settings{Role: "runnerfoundry"}, operatingSystemConnectionDriver())
	if NewLeaderElector(connections) == nil {
		t.Fatal("leader elector was not constructed")
	}
	session := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true)}}
	lease, err := leaderElectorWithSessions(fakeSessionOpener{connection: session}).TryAcquire(context.Background())
	if err != nil || lease == nil || !lease.Alive(context.Background()) {
		t.Fatalf("TryAcquire = (%#v, %v)", lease, err)
	}
	if err = lease.Close(context.Background()); err != nil || !session.closed {
		t.Fatalf("Close error = %v, closed=%t", err, session.closed)
	}
}

func TestLeaderElectorReportsContentionAndClosesSession(t *testing.T) {
	session := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, false)}}
	_, err := leaderElectorWithSessions(fakeSessionOpener{connection: session}).TryAcquire(context.Background())
	if !errors.Is(err, ErrNotLeader) || !session.closed {
		t.Fatalf("TryAcquire error = %v, closed=%t", err, session.closed)
	}
}

func TestLeaderElectorReportsSessionAndQueryFailures(t *testing.T) {
	failure := errors.New("failed")
	if _, err := leaderElectorWithSessions(fakeSessionOpener{err: failure}).TryAcquire(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("session error = %v", err)
	}
	session := &fakeDatabaseConnection{rowResults: []fakeRow{{err: failure}}, closeErr: failure}
	if _, err := leaderElectorWithSessions(fakeSessionOpener{connection: session}).TryAcquire(context.Background()); err == nil || !session.closed {
		t.Fatalf("query error = %v, closed=%t", err, session.closed)
	}
}

func TestLeaderLeaseLivenessAndCloseFailures(t *testing.T) {
	var nilLease *LeaderLease
	if nilLease.Alive(context.Background()) || nilLease.Close(context.Background()) != nil {
		t.Fatal("nil lease was not inert")
	}
	emptyLease := &LeaderLease{}
	if emptyLease.Alive(context.Background()) || emptyLease.Close(context.Background()) != nil {
		t.Fatal("empty lease was not inert")
	}
	session := &fakeDatabaseConnection{closed: true}
	lease := &LeaderLease{session: session.session()}
	if lease.Alive(context.Background()) {
		t.Fatal("closed lease reported alive")
	}
	session.closed = false
	session.pingErr = errors.New("ping")
	if lease.Alive(context.Background()) {
		t.Fatal("failed ping reported alive")
	}
	session.closeErr = errors.New("close")
	if err := lease.Close(context.Background()); err == nil {
		t.Fatal("close failure was hidden")
	}
}

func booleanRow(t *testing.T, value bool) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) { *destination[bool](t, destinations[0]) = value }}
}
