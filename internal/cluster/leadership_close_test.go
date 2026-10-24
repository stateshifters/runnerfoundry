package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLeaderLeaseCloseHonorsCallerCancellation(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	lease := &LeaderLease{session: connection.session()}
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
	lease := &LeaderLease{session: connection.session()}
	wantDeadline := time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)
	ctx := deadlineContext{deadline: wantDeadline}

	if err := lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !connection.closeHasDeadline || !connection.closeDeadline.Equal(wantDeadline) {
		t.Fatalf("close deadline = %v, %t; want %v, true", connection.closeDeadline, connection.closeHasDeadline, wantDeadline)
	}
}

type deadlineContext struct {
	deadline time.Time
}

func (ctx deadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }
func (deadlineContext) Done() <-chan struct{}           { return nil }
func (deadlineContext) Err() error                      { return nil }
func (deadlineContext) Value(any) any                   { return nil }
