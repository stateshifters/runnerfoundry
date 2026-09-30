//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func exerciseMembershipCapacityContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	cutoff := stateConformanceEpoch.Add(time.Hour)
	runnerPool := core.RunnerPool("state-conformance-capacity")
	publishStateConformanceMember(t, store, "member-stale", cutoff.Add(-time.Second), runnerPool, 20)
	publishStateConformanceMember(t, store, "member-cutoff", cutoff, runnerPool, 3)
	publishStateConformanceMember(t, store, "member-fresh", cutoff.Add(time.Second), runnerPool, 2)
	requireStateConformanceLiveMembers(t, store, cutoff)
	requireStateConformanceCapacity(t, store, runnerPool, cutoff, 5, "published capacity")
	exerciseCapacityDemand(t, store, runnerPool, cutoff)
	exerciseCapacityWithdrawal(t, store, runnerPool, cutoff)
}

func requireStateConformanceLiveMembers(t *testing.T, store stateConformanceStore, cutoff time.Time) {
	t.Helper()
	ctx := context.Background()
	members, err := store.LiveMemberIDs(ctx, cutoff)
	if err != nil || len(members) != 2 || members[0] != "member-cutoff" || members[1] != "member-fresh" {
		t.Fatalf("live members at inclusive cutoff = %#v, error = %v", members, err)
	}
	count, err := store.CountLiveMembers(ctx, cutoff)
	if err != nil || count != 2 {
		t.Fatalf("live member count at inclusive cutoff = %d, error = %v, want 2", count, err)
	}
}

func exerciseCapacityDemand(t *testing.T, store stateConformanceStore, runnerPool core.RunnerPool, cutoff time.Time) {
	t.Helper()
	firstRequest := stateConformanceRequest("capacity-first", runnerPool, cutoff.Add(2*time.Second))
	secondRequest := stateConformanceRequest("capacity-second", runnerPool, cutoff.Add(3*time.Second))
	requireQueued(t, store, firstRequest, core.QueueQueued)
	requireQueued(t, store, secondRequest, core.QueueQueued)
	requireClaimed(t, store, runnerPool, stateConformanceClaim("member-cutoff", cutoff.Add(4*time.Second)))
	requireStateConformanceCapacity(t, store, runnerPool, cutoff, 3, "queued and processing demand")
}

func exerciseCapacityWithdrawal(t *testing.T, store stateConformanceStore, runnerPool core.RunnerPool, cutoff time.Time) {
	t.Helper()
	ctx := context.Background()
	if err := store.WithdrawMemberCapacity(ctx, "member-fresh"); err != nil {
		t.Fatalf("withdraw member capacity: %v", err)
	}
	requireStateConformanceCapacity(t, store, runnerPool, cutoff, 1, "withdrawn member capacity")
	count, err := store.CountLiveMembers(ctx, cutoff)
	if err != nil || count != 2 {
		t.Fatalf("member count after capacity withdrawal = %d, error = %v, want 2", count, err)
	}
}

func publishStateConformanceMember(
	t *testing.T,
	store stateConformanceStore,
	member core.MemberID,
	heartbeat time.Time,
	runnerPool core.RunnerPool,
	capacity int,
) {
	t.Helper()
	err := store.PublishMemberState(context.Background(), member, heartbeat,
		[]core.RunnerPoolCapacity{{RunnerPool: runnerPool, Capacity: capacity}})
	if err != nil {
		t.Fatalf("publish member %q: %v", member, err)
	}
}

func requireStateConformanceCapacity(
	t *testing.T,
	store stateConformanceStore,
	runnerPool core.RunnerPool,
	cutoff time.Time,
	want int,
	description string,
) {
	t.Helper()
	capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), runnerPool, cutoff)
	if err != nil || capacity != want {
		t.Fatalf("%s = %d, error = %v, want %d", description, capacity, err, want)
	}
}
