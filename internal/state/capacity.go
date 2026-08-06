package state

// CalculateAvailableRunnerPoolCapacity subtracts waiting and all processing
// demand from the capacity published by live members.
func CalculateAvailableRunnerPoolCapacity(liveCapacity, queuedJobs, processingJobs int) int {
	available := liveCapacity - queuedJobs - processingJobs
	if available < 0 {
		return 0
	}
	return available
}
