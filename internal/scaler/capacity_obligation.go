package scaler

// AssignedCapacityStatistics contains the upstream counts needed to decide
// whether aggregate assigned capacity can still produce runnable work.
type AssignedCapacityStatistics struct {
	AssignedJobs      int
	RunningJobs       int
	RegisteredRunners int
	BusyRunners       int
}

// AssignedCapacityClassification separates authoritative assigned capacity
// into runnable work and assignments stranded in GitHub's aggregate state.
type AssignedCapacityClassification struct {
	StrandedAssigned int
	RunnableAssigned int
}

// ClassifyAssignedCapacity treats busy capacity as conclusively stranded only
// when GitHub reports no registered runner and no running job explains it.
func ClassifyAssignedCapacity(statistics AssignedCapacityStatistics) AssignedCapacityClassification {
	classification := AssignedCapacityClassification{RunnableAssigned: statistics.AssignedJobs}
	if statistics.RegisteredRunners != 0 {
		return classification
	}
	busyWithoutRunning := statistics.BusyRunners - statistics.RunningJobs
	if busyWithoutRunning <= 0 {
		return classification
	}
	classification.StrandedAssigned = busyWithoutRunning
	if classification.StrandedAssigned > statistics.AssignedJobs {
		classification.StrandedAssigned = statistics.AssignedJobs
	}
	classification.RunnableAssigned -= classification.StrandedAssigned
	return classification
}

// AssignedObligationSnapshot describes the capacity already accounted for when
// an authoritative assigned-job count is observed. ActiveCapacity includes
// exact and identity-free runners because GitHub's count does not distinguish
// how that capacity was admitted.
type AssignedObligationSnapshot struct {
	AssignedJobs       int
	StrandedAssigned   int
	ActiveCapacity     int
	QueuedObligations  int
	BlockedObligations int
}

// AssignedObligationDecision describes how the durable identity-free queue
// must change to represent an authoritative assigned-job count.
type AssignedObligationDecision struct {
	Create        int
	Cancel        int
	CancelBlocked int
	Block         int
	Unblock       int
}

// DecideAssignedObligations returns the minimum queue adjustment needed to
// make active and queued capacity equal the authoritative assigned-job count.
// Cancellation applies only to unclaimed obligations; lifecycle reconciliation
// remains responsible for excess waiting runners.
func DecideAssignedObligations(snapshot AssignedObligationSnapshot) AssignedObligationDecision {
	totalQueuedObligations := snapshot.QueuedObligations + snapshot.BlockedObligations
	accountedCapacity := snapshot.ActiveCapacity + totalQueuedObligations
	decision := decideAssignedObligationTotal(
		accountedCapacity, snapshot.AssignedJobs, totalQueuedObligations, snapshot.BlockedObligations,
	)
	remainingBlocked := snapshot.BlockedObligations - decision.CancelBlocked
	remainingQueued := totalQueuedObligations + decision.Create - decision.Cancel
	desiredBlocked := snapshot.StrandedAssigned
	if desiredBlocked > remainingQueued {
		desiredBlocked = remainingQueued
	}
	if desiredBlocked > remainingBlocked {
		decision.Block = desiredBlocked - remainingBlocked
	} else {
		decision.Unblock = remainingBlocked - desiredBlocked
	}
	return decision
}

func decideAssignedObligationTotal(
	accountedCapacity int,
	assignedJobs int,
	totalQueuedObligations int,
	blockedObligations int,
) AssignedObligationDecision {
	if accountedCapacity < assignedJobs {
		return AssignedObligationDecision{Create: assignedJobs - accountedCapacity}
	}
	canceled := min(accountedCapacity-assignedJobs, totalQueuedObligations)
	return AssignedObligationDecision{
		Cancel: canceled, CancelBlocked: min(canceled, blockedObligations),
	}
}
