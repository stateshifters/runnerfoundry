package scaler

import "testing"

func TestClassifyAssignedCapacitySeparatesStrandedAssignments(t *testing.T) {
	assertAssignedCapacityClassification(t, "busy without runner",
		AssignedCapacityStatistics{AssignedJobs: 1, BusyRunners: 1}, 1, 0)
	assertAssignedCapacityClassification(t, "busy surplus",
		AssignedCapacityStatistics{AssignedJobs: 2, BusyRunners: 1}, 1, 1)
	assertAssignedCapacityClassification(t, "capped by assigned",
		AssignedCapacityStatistics{AssignedJobs: 1, BusyRunners: 3}, 1, 0)
	assertAssignedCapacityClassification(t, "running explains busy",
		AssignedCapacityStatistics{AssignedJobs: 1, RunningJobs: 1, BusyRunners: 1}, 0, 1)
	assertAssignedCapacityClassification(t, "registered is inconclusive",
		AssignedCapacityStatistics{AssignedJobs: 1, RegisteredRunners: 1, BusyRunners: 1}, 0, 1)
}

func assertAssignedCapacityClassification(
	t *testing.T,
	name string,
	statistics AssignedCapacityStatistics,
	strandedAssigned int,
	runnableAssigned int,
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		classification := ClassifyAssignedCapacity(statistics)
		if classification.StrandedAssigned != strandedAssigned || classification.RunnableAssigned != runnableAssigned {
			t.Fatalf("classification = %#v, want stranded=%d runnable=%d",
				classification, strandedAssigned, runnableAssigned)
		}
	})
}

func TestAssignedObligationDecisionCreatesUnmetCapacity(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 7, ActiveCapacity: 3, QueuedObligations: 2,
	})
	if decision.Create != 2 || decision.Cancel != 0 {
		t.Fatalf("assigned obligation decision = %#v, want create 2", decision)
	}
}

func TestAssignedObligationDecisionCancelsNewestUnclaimedCapacity(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 2, ActiveCapacity: 2, QueuedObligations: 3,
	})
	if decision.Create != 0 || decision.Cancel != 3 {
		t.Fatalf("assigned obligation decision = %#v, want cancel 3", decision)
	}
}

func TestAssignedObligationDecisionCountsExactActiveCapacity(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 2, ActiveCapacity: 2,
	})
	if decision != (AssignedObligationDecision{}) {
		t.Fatalf("satisfied assigned obligation decision = %#v", decision)
	}
}

func TestAssignedObligationDecisionBlocksStrandedCapacity(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 2, StrandedAssigned: 1, QueuedObligations: 2,
	})
	if decision != (AssignedObligationDecision{Block: 1}) {
		t.Fatalf("assigned obligation decision = %#v, want block 1", decision)
	}
}

func TestAssignedObligationDecisionUnblocksRecoveredCapacity(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 2, QueuedObligations: 1, BlockedObligations: 1,
	})
	if decision != (AssignedObligationDecision{Unblock: 1}) {
		t.Fatalf("assigned obligation decision = %#v, want unblock 1", decision)
	}
}

func TestAssignedObligationDecisionCancelsBlockedCapacityFirst(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 1, QueuedObligations: 1, BlockedObligations: 1,
	})
	if decision != (AssignedObligationDecision{Cancel: 1, CancelBlocked: 1}) {
		t.Fatalf("assigned obligation decision = %#v, want one blocked cancellation", decision)
	}
}

func TestAssignedObligationDecisionCapsCancellationAtQueuedCapacity(t *testing.T) {
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: 0, StrandedAssigned: 3, ActiveCapacity: 5, QueuedObligations: 1,
	})
	if decision != (AssignedObligationDecision{Cancel: 1}) {
		t.Fatalf("capped assigned obligation decision = %#v", decision)
	}
}
