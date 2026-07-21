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
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := store.Publish(ctx, "spock", NodeHealthy, []Contribution{
		{ScaleSet: "shared", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
		{ScaleSet: "remote-only", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
	}, now); err != nil {
		t.Fatal(err)
	}
	for requestIndex, scaleSet := range []string{"shared", "remote-only"} {
		if err := store.Enqueue(ctx, Demand{
			ScaleSet: scaleSet, Repository: "repo", RequestID: int64(requestIndex + 1), QueueTime: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.TryLeader(ctx)
	if err != nil || first == nil {
		t.Fatalf("first leader lease = %#v, %v", first, err)
	}
	second, err := store.TryLeader(ctx)
	if !errors.Is(err, ErrNotLeader) || second != nil {
		t.Fatalf("second leader lease = %#v, %v", second, err)
	}
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
	select {
	case second = <-waiting:
	case waitErr := <-waitingErrors:
		t.Fatal(waitErr)
	case <-time.After(2 * time.Second):
		t.Fatal("blocking leader contender did not acquire the released lease")
	}
	if closeErr := second.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
	for requestIndex, scaleSet := range []string{"shared", "remote-only"} {
		assignment, assignErr := store.AssignOldest(ctx, scaleSet, now, time.Minute)
		if assignErr != nil || assignment == nil || assignment.RequestID != int64(requestIndex+1) {
			t.Fatalf("failover lost %q demand: %#v, %v", scaleSet, assignment, assignErr)
		}
	}
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
