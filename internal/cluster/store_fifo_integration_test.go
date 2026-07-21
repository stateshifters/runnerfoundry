//go:build integration

package cluster

import (
	"errors"
	"testing"
	"time"
)

func TestPostgreSQLPreservesGlobalFIFOWithinHostLimits(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	for requestID := int64(1); requestID <= 13; requestID++ {
		repository := "https://github.com/example/alpha"
		if requestID%2 == 0 {
			repository = "https://github.com/example/beta"
		}
		if err := fixture.store.Enqueue(fixture.ctx, Demand{
			ScaleSet: "meza-ubuntu-slim", Repository: repository, RequestID: requestID,
			QueueTime: fixture.now.Add(time.Duration(requestID) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	assignmentsByNode := map[NodeID]int{}
	for requestID := int64(1); requestID <= 12; requestID++ {
		assignment, err := fixture.store.AssignOldest(fixture.ctx, "meza-ubuntu-slim", fixture.now, fixture.liveFor)
		if err != nil {
			t.Fatal(err)
		}
		if assignment.RequestID != requestID {
			t.Fatalf("assignment %d = %#v; global FIFO was not preserved", requestID, assignment)
		}
		assignmentsByNode[assignment.NodeID]++
	}
	if assignmentsByNode["spock"] != 10 || assignmentsByNode["serenity"] != 2 {
		t.Fatalf("local limits exceeded or underused: %#v", assignmentsByNode)
	}
	assignment, err := fixture.store.AssignOldest(fixture.ctx, "meza-ubuntu-slim", fixture.now, fixture.liveFor)
	if !errors.Is(err, ErrNoAssignment) || assignment != nil {
		t.Fatalf("expected queued demand after aggregate capacity filled, got %#v, %v", assignment, err)
	}
}

func TestPostgreSQLAssignmentTransitionsPreserveRecoveryMetadataAndReleaseCapacity(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	for requestID := int64(1); requestID <= 2; requestID++ {
		if err := fixture.store.Enqueue(fixture.ctx, Demand{
			ScaleSet: "meza-ubuntu-slim", Repository: "repo", RequestID: requestID,
			QueueTime: fixture.now.Add(time.Duration(requestID) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	assignment, err := fixture.store.AssignOldest(fixture.ctx, "meza-ubuntu-slim", fixture.now, fixture.liveFor)
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.store.BeginAcquisition(fixture.ctx, assignment.ID, 42, 99); err != nil {
		t.Fatal(err)
	}
	pending, err := fixture.store.PendingAcquisitions(fixture.ctx)
	if err != nil || len(pending) != 1 || pending[0].State != AssignmentAcquiring ||
		pending[0].InstallationID != 42 || pending[0].GitHubScaleSetID != 99 {
		t.Fatalf("pending acquisition = %#v, %v", pending, err)
	}
	if err = fixture.store.ConfirmAcquisition(fixture.ctx, assignment.ID); err != nil {
		t.Fatal(err)
	}
	if err = fixture.store.ConfirmAcquisition(fixture.ctx, assignment.ID); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("repeated confirmation error = %v", err)
	}
	claimed, err := fixture.store.ClaimForStart(fixture.ctx, assignment.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.InstallationID != 42 || claimed.GitHubScaleSetID != 99 {
		t.Fatalf("claimed assignment lost metadata: %#v", claimed)
	}
	if err = fixture.store.ConfirmRunning(fixture.ctx, claimed.ID, "wrong-node"); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("wrong-owner confirmation error = %v", err)
	}
	if err = fixture.store.ConfirmRunning(fixture.ctx, claimed.ID, assignment.NodeID); err != nil {
		t.Fatal(err)
	}
	if err = fixture.store.CompleteDemand(fixture.ctx, claimed.ScaleSet, claimed.Repository, claimed.RequestID); err != nil {
		t.Fatal(err)
	}
	next, err := fixture.store.AssignOldest(fixture.ctx, "meza-ubuntu-slim", fixture.now, fixture.liveFor)
	if err != nil || next.RequestID != 2 {
		t.Fatalf("released capacity did not admit next demand: %#v, %v", next, err)
	}
}
