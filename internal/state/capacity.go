package state

// CalculateAvailableRunnerPoolCapacity subtracts durable active demand from
// live configured capacity. Terminal results are absent from the inputs
// because they no longer require a runner.
func CalculateAvailableRunnerPoolCapacity(liveCapacity, queuedJobs, attemptingJobs int) int {
	available := liveCapacity - queuedJobs - attemptingJobs
	if available < 0 {
		return 0
	}
	return available
}
