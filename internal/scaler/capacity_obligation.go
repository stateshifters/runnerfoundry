package scaler

// AssignedObligationSnapshot describes the capacity already accounted for when
// an authoritative assigned-job count is observed. ActiveCapacity includes
// exact and identity-free runners because GitHub's count does not distinguish
// how that capacity was admitted.
type AssignedObligationSnapshot struct {
	AssignedJobs      int
	ActiveCapacity    int
	QueuedObligations int
}

// AssignedObligationDecision describes how the durable identity-free queue
// must change to represent an authoritative assigned-job count.
type AssignedObligationDecision struct {
	Create int
	Cancel int
}

// DecideAssignedObligations returns the minimum queue adjustment needed to
// make active and queued capacity equal the authoritative assigned-job count.
// Cancellation applies only to unclaimed obligations; lifecycle reconciliation
// remains responsible for excess waiting runners.
func DecideAssignedObligations(snapshot AssignedObligationSnapshot) AssignedObligationDecision {
	accountedCapacity := snapshot.ActiveCapacity + snapshot.QueuedObligations
	if accountedCapacity < snapshot.AssignedJobs {
		return AssignedObligationDecision{Create: snapshot.AssignedJobs - accountedCapacity}
	}
	excessCapacity := accountedCapacity - snapshot.AssignedJobs
	if excessCapacity > snapshot.QueuedObligations {
		excessCapacity = snapshot.QueuedObligations
	}
	return AssignedObligationDecision{Cancel: excessCapacity}
}
