package state_test

import (
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
	"testing"
	"time"
)

func TestProcessingRetryRejectsChangedAttemptBeforeLifecycle(t *testing.T) {
	t.Parallel()
	expected := retryClassificationClaim()
	for name, change := range map[string]func(*core.ProcessingJob){
		"job":        func(job *core.ProcessingJob) { job.Job.ID = core.JobID{2} },
		"runner":     func(job *core.ProcessingJob) { job.RunnerName = "replacement" },
		"claim time": func(job *core.ProcessingJob) { job.ClaimedAt = job.ClaimedAt.Add(time.Nanosecond) },
		"endpoint":   func(job *core.ProcessingJob) { job.InventoryEndpointID = "replacement" },
	} {
		t.Run(name, func(t *testing.T) {
			actual := expected
			actual.SourceTerminal = true
			change(&actual)
			if outcome := state.ClassifyProcessingRetry(actual, expected); outcome != core.RetryClaimChanged {
				t.Fatalf("changed claim retry = %q", outcome)
			}
		})
	}
}

func TestProcessingRetryClassifiesLifecycleInOrder(t *testing.T) {
	t.Parallel()
	expected := retryClassificationClaim()
	for name, scenario := range map[string]struct {
		accepted, terminal bool
		failure            core.ProvisioningFailureState
		want               core.RetryOutcome
	}{
		"terminal precedes accepted": {terminal: true, accepted: true, failure: core.ProvisioningFailureReported, want: core.RetrySourceTerminal},
		"accepted precedes failure":  {accepted: true, failure: core.ProvisioningFailureReported, want: core.RetryRunnerAccepted},
		"failure reported":           {failure: core.ProvisioningFailureReported, want: core.RetryFailureFinalizationPending},
		"failure reporting":          {failure: core.ProvisioningFailureReporting, want: core.RetryFailureReportingPending},
		"unstarted permits retry":    {},
	} {
		t.Run(name, func(t *testing.T) {
			actual := expected
			actual.SourceTerminal = scenario.terminal
			actual.RunnerAccepted = scenario.accepted
			actual.ProvisioningFailure = scenario.failure
			if outcome := state.ClassifyProcessingRetry(actual, expected); outcome != scenario.want {
				t.Fatalf("retry classification = %q, want %q", outcome, scenario.want)
			}
		})
	}
}

func retryClassificationClaim() core.ProcessingJob {
	return core.ProcessingJob{Job: core.Job{ID: core.JobID{1}}, ClaimedAt: time.Unix(100, 0).UTC(), RunnerName: "attempt", InventoryEndpointID: "endpoint"}
}
