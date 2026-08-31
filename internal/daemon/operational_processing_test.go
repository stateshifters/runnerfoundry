package daemon

import (
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestPoolOperationalEventsReportRetainedAndTerminalProvisioningFailures(t *testing.T) {
	events := make([]operationalEvent, 0, 4)
	report := func(err error) {
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("report = %T, want operational envelope", err)
		}
		events = append(events, envelope.event)
	}
	jobID := core.JobID{1}
	failure := errors.New("provisioning failed")
	reportProcessOutcome(report, "linux", "docker", "docker:test", "sqlite", runtimeengine.ProcessResult{
		Outcome: runtimeengine.ProcessClaimed, JobID: jobID, FailedStage: "resolve_source_operations",
	}, failure)
	reportProcessOutcome(report, "linux", "docker", "docker:test", "sqlite", runtimeengine.ProcessResult{
		Outcome: runtimeengine.ProcessProvisioningFailureReported, JobID: jobID, FailedStage: "generate_bootstrap",
	}, failure)
	if len(events) != 4 || events[1].operation != "job_processing" || events[1].outcome != "failed" ||
		events[1].stage != "resolve_source_operations" || !errors.Is(events[1].err, failure) ||
		events[3].operation != "provisioning_failure" || events[3].outcome != "reported" ||
		events[3].stage != provisioningFailureReportingStage || events[3].err != nil {
		t.Fatalf("events = %#v", events)
	}
}

func TestPendingProvisioningFailureMessageDescribesRetainedProcessing(t *testing.T) {
	message := runnerRetryMessage(operationalEvent{
		outcome:    string(core.RetryFailureReportingPending),
		stage:      "start_runner",
		runnerPool: "linux",
		jobID:      core.JobID{1},
		err:        errors.New("report failed"),
	})
	if message != "Retained job 01000000-0000-0000-0000-000000000000 in Runner Pool linux while provisioning failure reporting remains pending" {
		t.Fatalf("message = %q", message)
	}
}

func TestReconciliationReportsReplayedProvisioningFailures(t *testing.T) {
	events := make([]operationalEvent, 0, 1)
	report := func(err error) {
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("report = %T, want operational envelope", err)
		}
		events = append(events, envelope.event)
	}

	reportReconciliationOutcome(report, "linux", "docker", "docker:test", runtimeengine.PoolReconciliation{
		ReportedProvisioningFailures: []core.Job{{ID: core.JobID{1}, RunnerPool: "linux"}},
	})

	if len(events) != 2 || events[0].operation != "provisioning_failure" ||
		events[0].stage != provisioningFailureReportingStage ||
		events[0].outcome != "reported" || events[0].count != 1 || events[0].runnerPool != "linux" ||
		events[0].provider != "docker" || events[0].endpoint != "docker:test" ||
		events[1].operation != "job_processing" || events[1].stage != provisioningFailureFinalizationStage ||
		events[1].outcome != "recovered" || events[1].jobID != (core.JobID{1}) {
		t.Fatalf("events = %#v", events)
	}
}

func provisioningFailureOperationalJob(t *testing.T, jobID core.JobID) core.Job {
	t.Helper()
	sourceID, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: "https://github.com/example/project", RunnerPool: "linux", RequestID: 42,
	})
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	return core.Job{ID: jobID, RunnerPool: "linux",
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}}
}

func TestProvisioningFailureRecoveryIsPerJobAndRestoresHealth(t *testing.T) {
	for name, test := range provisioningFailureRecoveryCases() {
		t.Run(name, func(t *testing.T) { assertProvisioningFailureRecovery(t, test) })
	}
}

type provisioningFailureRecoveryCase struct {
	outcome        core.RetryOutcome
	failure        error
	reconciliation func([]core.Job) runtimeengine.PoolReconciliation
}

func provisioningFailureRecoveryCases() map[string]provisioningFailureRecoveryCase {
	return map[string]provisioningFailureRecoveryCase{
		"source report": {
			outcome: core.RetryFailureReportingPending,
			failure: errors.New("report failed"),
			reconciliation: func(jobs []core.Job) runtimeengine.PoolReconciliation {
				return runtimeengine.PoolReconciliation{ReportedProvisioningFailures: jobs}
			},
		},
		"internal finalization": {
			outcome: core.RetryFailureFinalizationPending,
			failure: errors.New("remove terminal processing"),
			reconciliation: func(jobs []core.Job) runtimeengine.PoolReconciliation {
				return runtimeengine.PoolReconciliation{FinalizedTerminalJobs: jobs}
			},
		},
	}
}

func assertProvisioningFailureRecovery(t *testing.T, test provisioningFailureRecoveryCase) {
	t.Helper()
	reporter := newDebugRuntimeReporter(t, &memoryLogDestination{})
	notifier := &recordingStatusNotifier{}
	reporter.notifier = notifier
	jobs := []core.Job{
		provisioningFailureOperationalJob(t, core.JobID{1}),
		provisioningFailureOperationalJob(t, core.JobID{2}),
	}
	for _, job := range jobs {
		reportProcessOutcome(reporter.reportMaintenance, "linux", "docker", "docker:test", "sqlite",
			runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry,
				RetryOutcome: test.outcome, JobID: job.ID, Job: job,
				FailedStage: "generate_bootstrap"}, test.failure)
	}
	assertProvisioningFailureHealth(t, reporter, notifier, 2, healthDegraded)

	reportReconciliationOutcome(reporter.reportMaintenance, "linux", "docker", "docker:test",
		test.reconciliation(jobs[:1]))
	assertProvisioningFailureHealth(t, reporter, notifier, 1, healthDegraded)

	reportReconciliationOutcome(reporter.reportMaintenance, "linux", "docker", "docker:test",
		test.reconciliation(jobs[1:]))
	assertProvisioningFailureHealth(t, reporter, notifier, 0, healthOperational)
}

func assertProvisioningFailureHealth(
	t *testing.T,
	reporter *productionRuntimeReporter,
	notifier *recordingStatusNotifier,
	wantFailures int,
	wantHealth operationalHealth,
) {
	t.Helper()
	if len(reporter.failures) != wantFailures || notifier.statuses[len(notifier.statuses)-1].health != wantHealth {
		t.Fatalf("operational health: failures=%#v statuses=%#v", reporter.failures, notifier.statuses)
	}
}

func TestImmediatelyReportedProvisioningFailureDoesNotDegradeHealth(t *testing.T) {
	reporter := newDebugRuntimeReporter(t, &memoryLogDestination{})
	notifier := &recordingStatusNotifier{}
	reporter.notifier = notifier

	reportProcessOutcome(reporter.reportMaintenance, "linux", "docker", "docker:test", "sqlite",
		runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessProvisioningFailureReported,
			JobID: core.JobID{1}, FailedStage: "generate_bootstrap"}, errors.New("bootstrap failed"))
	if len(reporter.failures) != 0 || notifier.statuses[len(notifier.statuses)-1].health != healthOperational {
		t.Fatalf("reported failure: failures=%#v statuses=%#v", reporter.failures, notifier.statuses)
	}
}
