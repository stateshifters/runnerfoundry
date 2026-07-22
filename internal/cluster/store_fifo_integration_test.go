//go:build integration

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgreSQLPreservesGlobalFIFOWithinHostLimits(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	fixture.enqueueAlternatingDemand(t, 13)
	assignmentsByNode := fixture.assertFIFOAssignments(t, 12)
	if assignmentsByNode["spock"] != 10 || assignmentsByNode["serenity"] != 2 {
		t.Fatalf("local limits exceeded or underused: %#v", assignmentsByNode)
	}
	assignment, err := fixture.store.AssignOldest(context.Background(), "meza-ubuntu-slim", fixture.now, fixture.liveFor)
	if !errors.Is(err, ErrNoAssignment) || assignment != nil {
		t.Fatalf("expected queued demand after aggregate capacity filled, got %#v, %v", assignment, err)
	}
}

func TestPostgreSQLPendingAcquisitionPreservesRecoveryMetadata(t *testing.T) {
	fixture := newAssignmentTransitionFixture(t)
	fixture.beginAcquisition(t)
	pending, err := fixture.store.PendingAcquisitions(context.Background())
	if err != nil || len(pending) != 1 || pending[0].State != AssignmentAcquiring ||
		pending[0].InstallationID != 42 || pending[0].GitHubScaleSetID != 99 {
		t.Fatalf("pending acquisition = %#v, %v", pending, err)
	}
}

func TestPostgreSQLRepeatedAcquisitionConfirmationConflicts(t *testing.T) {
	fixture := newAssignmentTransitionFixture(t)
	fixture.beginAcquisition(t)
	fixture.confirmAcquisition(t)
	if err := fixture.store.ConfirmAcquisition(context.Background(), fixture.assignment.ID); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("repeated confirmation error = %v", err)
	}
}

func TestPostgreSQLClaimForStartPreservesRecoveryMetadata(t *testing.T) {
	fixture := newAssignmentTransitionFixture(t)
	claimed := fixture.claimForStart(t)
	if claimed.InstallationID != 42 || claimed.GitHubScaleSetID != 99 {
		t.Fatalf("claimed assignment lost metadata: %#v", claimed)
	}
}

func TestPostgreSQLRunningConfirmationRejectsWrongOwner(t *testing.T) {
	fixture := newAssignmentTransitionFixture(t)
	claimed := fixture.claimForStart(t)
	if err := fixture.store.ConfirmRunning(context.Background(), claimed.ID, "wrong-node"); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("wrong-owner confirmation error = %v", err)
	}
}

func TestPostgreSQLCompletionReleasesCapacityForNextDemand(t *testing.T) {
	fixture := newAssignmentTransitionFixture(t)
	claimed := fixture.claimForStart(t)
	if err := fixture.store.ConfirmRunning(context.Background(), claimed.ID, fixture.assignment.NodeID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.CompleteDemand(context.Background(), claimed.ScaleSet, claimed.Repository, claimed.RequestID); err != nil {
		t.Fatal(err)
	}
	next, err := fixture.store.AssignOldest(context.Background(), "meza-ubuntu-slim", fixture.now, fixture.liveFor)
	if err != nil || next.RequestID != 2 {
		t.Fatalf("released capacity did not admit next demand: %#v, %v", next, err)
	}
}

type assignmentTransitionFixture struct {
	capacityTestCluster
	assignment *Assignment
}

func newAssignmentTransitionFixture(t *testing.T) assignmentTransitionFixture {
	t.Helper()
	fixture := newCapacityTestCluster(t)
	for requestID := int64(1); requestID <= 2; requestID++ {
		if err := fixture.store.Enqueue(context.Background(), Demand{
			ScaleSet: "meza-ubuntu-slim", Repository: "repo", RequestID: requestID,
			QueueTime: fixture.now.Add(time.Duration(requestID) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	assignment, err := fixture.store.AssignOldest(context.Background(), "meza-ubuntu-slim", fixture.now, fixture.liveFor)
	if err != nil {
		t.Fatal(err)
	}
	return assignmentTransitionFixture{capacityTestCluster: fixture, assignment: assignment}
}

func (fixture assignmentTransitionFixture) beginAcquisition(t *testing.T) {
	t.Helper()
	if err := fixture.store.BeginAcquisition(context.Background(), fixture.assignment.ID, 42, 99); err != nil {
		t.Fatal(err)
	}
}

func (fixture assignmentTransitionFixture) confirmAcquisition(t *testing.T) {
	t.Helper()
	if err := fixture.store.ConfirmAcquisition(context.Background(), fixture.assignment.ID); err != nil {
		t.Fatal(err)
	}
}

func (fixture assignmentTransitionFixture) claimForStart(t *testing.T) *Assignment {
	t.Helper()
	fixture.beginAcquisition(t)
	fixture.confirmAcquisition(t)
	claimed, err := fixture.store.ClaimForStart(context.Background(), fixture.assignment.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}

func (fixture capacityTestCluster) enqueueAlternatingDemand(t *testing.T, count int64) {
	t.Helper()
	for requestID := int64(1); requestID <= count; requestID++ {
		repository := "https://github.com/example/alpha"
		if requestID%2 == 0 {
			repository = "https://github.com/example/beta"
		}
		if err := fixture.store.Enqueue(context.Background(), Demand{
			ScaleSet: "meza-ubuntu-slim", Repository: repository, RequestID: requestID,
			QueueTime: fixture.now.Add(time.Duration(requestID) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func (fixture capacityTestCluster) assertFIFOAssignments(t *testing.T, count int64) map[NodeID]int {
	t.Helper()
	assignmentsByNode := map[NodeID]int{}
	for requestID := int64(1); requestID <= count; requestID++ {
		assignment, err := fixture.store.AssignOldest(context.Background(), "meza-ubuntu-slim", fixture.now, fixture.liveFor)
		if err != nil {
			t.Fatal(err)
		}
		if assignment.RequestID != requestID {
			t.Fatalf("assignment %d = %#v; global FIFO was not preserved", requestID, assignment)
		}
		assignmentsByNode[assignment.NodeID]++
	}
	return assignmentsByNode
}
