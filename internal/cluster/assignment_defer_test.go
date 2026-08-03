package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDeferAssignmentReportsDatabaseFailure(t *testing.T) {
	failure := databaseFailure()
	store := storeWithConnection(&fakeDatabaseConnection{execErrors: []error{failure}})
	err := store.DeferAssignment(context.Background(), 1, "node", AssignmentStarting, time.Now())
	if !errors.Is(err, failure) {
		t.Fatalf("defer database error = %v, want %v", err, failure)
	}
}

func TestDeferAssignmentReportsTransitionMismatch(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{execTag: pgconn.NewCommandTag("UPDATE 0")})
	err := store.DeferAssignment(context.Background(), 2, "node", AssignmentStarting, time.Now())
	var transitionErr *AssignmentTransitionError
	if !errors.As(err, &transitionErr) || transitionErr.AssignmentID != 2 || transitionErr.Expected != AssignmentStarting {
		t.Fatalf("defer transition error = %#v", err)
	}
}
