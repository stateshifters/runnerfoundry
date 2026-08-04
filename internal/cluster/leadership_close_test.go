package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLeaderLeaseCloseHonorsCallerCancellation(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	lease := &LeaderLease{connection: connection}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(connection.closeContextErr, context.Canceled) {
		t.Fatalf("close context error = %v, want canceled", connection.closeContextErr)
	}
}

func TestLeaderLeaseClosePropagatesCallerDeadline(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	lease := &LeaderLease{connection: connection}
	wantDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), wantDeadline)
	defer cancel()

	if err := lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !connection.closeHasDeadline || !connection.closeDeadline.Equal(wantDeadline) {
		t.Fatalf("close deadline = %v, %t; want %v, true", connection.closeDeadline, connection.closeHasDeadline, wantDeadline)
	}
}
