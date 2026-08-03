package scaler

import "testing"

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
