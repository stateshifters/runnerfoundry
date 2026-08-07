package cluster

import (
	"context"
	"errors"
	"testing"
)

func TestTryLeaderReportsConnectionFailure(t *testing.T) {
	failure := errors.New("connect")
	store := NewStore(Settings{})
	store.directConnector = func(context.Context) (leaderConnection, error) { return nil, failure }
	if _, err := store.TryLeader(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("TryLeader error = %v", err)
	}
}

func TestTryLeaderReportsQueryFailure(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{{err: errors.New("query")}}}
	if _, err := storeWithConnection(connection).TryLeader(context.Background()); err == nil || !connection.closed {
		t.Fatalf("TryLeader error = %v, closed=%t", err, connection.closed)
	}
}

func TestTryLeaderReportsStandby(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, false)}}
	if _, err := storeWithConnection(connection).TryLeader(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("TryLeader error = %v", err)
	}
}

func TestTryLeaderReturnsLease(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true)}}
	lease, err := storeWithConnection(connection).TryLeader(context.Background())
	if err != nil || lease == nil {
		t.Fatalf("TryLeader = (%#v, %v)", lease, err)
	}
}

func TestWaitForLeaderReportsConnectionFailure(t *testing.T) {
	failure := errors.New("connect")
	store := NewStore(Settings{})
	store.directConnector = func(context.Context) (leaderConnection, error) { return nil, failure }
	if _, err := store.WaitForLeader(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("WaitForLeader error = %v", err)
	}
}

func TestWaitForLeaderReportsExecutionFailure(t *testing.T) {
	assertWaitForLeaderFailure(context.Background(), t)
}

func TestWaitForLeaderReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertWaitForLeaderFailure(ctx, t)
}

func assertWaitForLeaderFailure(ctx context.Context, t *testing.T) {
	t.Helper()
	connection := &fakeDatabaseConnection{execErrors: []error{errors.New("lock")}}
	if _, err := storeWithConnection(connection).WaitForLeader(ctx); err == nil || !connection.closed {
		t.Fatalf("WaitForLeader error = %v, closed=%t", err, connection.closed)
	}
}

func TestWaitForLeaderReturnsLease(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	lease, err := storeWithConnection(connection).WaitForLeader(context.Background())
	if err != nil || lease == nil {
		t.Fatalf("WaitForLeader = (%#v, %v)", lease, err)
	}
}

func TestLeaderLeaseLivenessAndCloseBoundaries(t *testing.T) {
	var nilLease *LeaderLease
	if nilLease.Alive(context.Background()) || nilLease.Close(context.Background()) != nil {
		t.Fatal("nil lease was not inert")
	}
	emptyLease := &LeaderLease{}
	if emptyLease.Alive(context.Background()) || emptyLease.Close(context.Background()) != nil {
		t.Fatal("empty lease was not inert")
	}

	connection := &fakeDatabaseConnection{closed: true}
	lease := &LeaderLease{connection: connection}
	if lease.Alive(context.Background()) {
		t.Fatal("closed lease reported alive")
	}
	connection.closed = false
	connection.pingErr = errors.New("ping")
	if lease.Alive(context.Background()) {
		t.Fatal("failed ping reported alive")
	}
	connection.pingErr = nil
	if !lease.Alive(context.Background()) {
		t.Fatal("healthy lease reported unavailable")
	}
	connection.closeErr = errors.New("close")
	if err := lease.Close(context.Background()); err == nil {
		t.Fatal("close failure was hidden")
	}
}

func TestConnectionCloseHelpersReportFailures(t *testing.T) {
	connection := &fakeDatabaseConnection{closeErr: errors.New("close")}
	if err := closeConnection(context.Background(), connection); err == nil {
		t.Fatal("closeConnection hid failure")
	}
	if err := closeConnectionWithContext(context.Background(), connection); err == nil {
		t.Fatal("closeConnectionWithContext hid failure")
	}
}
