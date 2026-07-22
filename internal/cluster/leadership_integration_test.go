//go:build integration

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLLeaderLeaseFailsOver(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	ctx := context.Background()
	first := acquireLeader(t, store)
	waiting := make(chan *LeaderLease, 1)
	waitingErrors := make(chan error, 1)
	go func() {
		lease, waitErr := store.WaitForLeader(ctx)
		if waitErr != nil {
			waitingErrors <- waitErr
			return
		}
		waiting <- lease
	}()
	if closeErr := first.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
	second := receiveWaitingLeader(t, waiting, waitingErrors)
	if closeErr := second.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
}

func TestPostgreSQLTryLeaderRejectsConcurrentLease(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	first := acquireLeader(t, store)
	t.Cleanup(func() {
		if closeErr := first.Close(context.Background()); closeErr != nil {
			t.Errorf("close first leader lease: %v", closeErr)
		}
	})
	second, err := store.TryLeader(context.Background())
	if !errors.Is(err, ErrNotLeader) || second != nil {
		t.Fatalf("second leader lease = %#v, %v", second, err)
	}
}

func TestPostgreSQLLeaderFailoverPreservesDemand(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	now := seedLeadershipDemand(t, store)
	first := acquireLeader(t, store)
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := acquireLeader(t, store)
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for requestIndex, scaleSet := range []string{"shared", "remote-only"} {
		assignment, assignErr := store.AssignOldest(context.Background(), scaleSet, now, time.Minute)
		if assignErr != nil || assignment == nil || assignment.RequestID != int64(requestIndex+1) {
			t.Fatalf("failover lost %q demand: %#v, %v", scaleSet, assignment, assignErr)
		}
	}
}

func TestPostgreSQLDatabaseAdapterRejectsClosedConnection(t *testing.T) {
	_, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	databaseConnection := newPostgreSQLDatabaseConnection(connection)
	if _, err = databaseConnection.Begin(ctx); err == nil {
		t.Fatal("expected a closed PostgreSQL connection to reject transactions")
	}
	if _, err = databaseConnection.Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("expected a closed PostgreSQL connection to reject queries")
	}
}

func acquireLeader(t *testing.T, store *Store) *LeaderLease {
	t.Helper()
	lease, err := store.TryLeader(context.Background())
	if err != nil || lease == nil {
		t.Fatalf("leader lease = %#v, %v", lease, err)
	}
	return lease
}

func receiveWaitingLeader(
	t *testing.T, waiting <-chan *LeaderLease, waitingErrors <-chan error,
) *LeaderLease {
	t.Helper()
	select {
	case lease := <-waiting:
		return lease
	case err := <-waitingErrors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("blocking leader contender did not acquire the released lease")
	}
	return nil
}

func seedLeadershipDemand(t *testing.T, store *Store) time.Time {
	t.Helper()
	now := time.Now()
	if err := store.Publish(context.Background(), "spock", NodeHealthy, []Contribution{
		{ScaleSet: "shared", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
		{ScaleSet: "remote-only", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
	}, now); err != nil {
		t.Fatal(err)
	}
	for requestIndex, scaleSet := range []string{"shared", "remote-only"} {
		if err := store.Enqueue(context.Background(), Demand{
			ScaleSet: scaleSet, Repository: "repo", RequestID: int64(requestIndex + 1), QueueTime: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return now
}
