package storetest

import (
	"reflect"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

type Scenario string

const (
	QueueNew             Scenario = "queue/new"
	QueueDuplicate       Scenario = "queue/duplicate"
	QueueProcessing      Scenario = "queue/processing"
	QueueTerminal        Scenario = "queue/source-terminal"
	ClaimFIFO            Scenario = "claim/fifo-by-pool"
	ClaimEmpty           Scenario = "claim/empty"
	ClaimMetadata        Scenario = "claim/metadata"
	DelayedStart         Scenario = "lifecycle/delayed-start"
	RetryRequeue         Scenario = "retry/requeued"
	RetryMissing         Scenario = "retry/missing"
	RetryStarted         Scenario = "retry/runner-started"
	RetryTerminal        Scenario = "retry/source-terminal"
	RetryClaimChanged    Scenario = "retry/claim-changed"
	RecoveryRequeue      Scenario = "recovery/requeued"
	RecoveryMissing      Scenario = "recovery/missing"
	RecoveryStarted      Scenario = "recovery/runner-started"
	RecoveryTerminal     Scenario = "recovery/source-terminal"
	RecoveryClaimChanged Scenario = "recovery/claim-changed"
	RemovalRemoved       Scenario = "removal/removed"
	RemovalMissing       Scenario = "removal/missing"
	RemovalNotTerminal   Scenario = "removal/not-terminal"
	Capacity             Scenario = "capacity/calculation"
	RegistrationRecords  Scenario = "registration/independent-records"
)

type Observation struct {
	QueueOutcome   core.QueueOutcome
	ClaimOutcome   core.ClaimOutcome
	RetryOutcome   core.RetryOutcome
	RemovalOutcome core.TerminalRemovalOutcome
	Job            core.Job
	Processing     core.ProcessingJob
	Capacity       int
	Started        bool
	Registrations  githubsource.RegistrationSnapshot
}

type Factory interface {
	Exercise(t *testing.T, scenario Scenario) Observation
}

func RunConformance(t *testing.T, factory Factory) {
	t.Helper()
	for scenario, want := range conformanceExpectations {
		t.Run(string(scenario), func(t *testing.T) {
			assertObservation(t, factory.Exercise(t, scenario), want)
		})
	}
}

type fatalReporter interface {
	Fatalf(format string, arguments ...any)
}

func assertObservation(reporter fatalReporter, got, want Observation) {
	if !reflect.DeepEqual(got, want) {
		reporter.Fatalf("observation = %#v, want %#v", got, want)
	}
}

var conformanceExpectations = map[Scenario]Observation{
	QueueNew:        {QueueOutcome: core.QueueQueued},
	QueueDuplicate:  {QueueOutcome: core.QueueAlreadyQueued},
	QueueProcessing: {QueueOutcome: core.QueueProcessing},
	QueueTerminal:   {QueueOutcome: core.QueueSourceTerminal},
	ClaimFIFO:       {ClaimOutcome: core.ClaimClaimed, Job: OldestJob()},
	ClaimEmpty:      {ClaimOutcome: core.ClaimEmpty},
	ClaimMetadata:   {ClaimOutcome: core.ClaimClaimed, Job: OldestJob(), Processing: ExpectedProcessing()},
	DelayedStart:    {Started: true},
	RetryRequeue:    {RetryOutcome: core.RetryRequeued}, RetryMissing: {RetryOutcome: core.RetryMissing},
	RetryStarted: {RetryOutcome: core.RetryRunnerStarted}, RetryTerminal: {RetryOutcome: core.RetrySourceTerminal},
	RetryClaimChanged: {RetryOutcome: core.RetryClaimChanged},
	RecoveryRequeue:   {RetryOutcome: core.RetryRequeued}, RecoveryMissing: {RetryOutcome: core.RetryMissing},
	RecoveryStarted: {RetryOutcome: core.RetryRunnerStarted}, RecoveryTerminal: {RetryOutcome: core.RetrySourceTerminal},
	RecoveryClaimChanged: {RetryOutcome: core.RetryClaimChanged},
	RemovalRemoved:       {RemovalOutcome: core.TerminalRemoved}, RemovalMissing: {RemovalOutcome: core.TerminalMissing},
	RemovalNotTerminal: {RemovalOutcome: core.TerminalNotTerminal},
	Capacity:           {Capacity: 2}, RegistrationRecords: {Registrations: ExpectedRegistrations()},
}

func OldestJob() core.Job {
	return core.Job{Source: core.SourceReference{Adapter: "github", ID: "older"},
		RunnerPool: "linux", EnqueuedAt: FixedTime()}
}

func ExpectedProcessing() core.ProcessingJob {
	job := OldestJob()
	return core.ProcessingJob{Job: job, RunnerName: core.AttemptRunnerName(job.ID, core.JobID{}),
		MemberID: "member-a", ClaimedAt: FixedTime(),
		InventoryEndpointID: "docker:member-a"}
}

func ExpectedRegistrations() githubsource.RegistrationSnapshot {
	active := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "https://example.test/active", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	owned := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "https://example.test/owned", RunnerPool: "linux",
		InstallationID: 4, RunnerGroupID: 5, ScaleSetID: 6}
	return githubsource.RegistrationSnapshot{Active: []githubsource.Registration{active}, Owned: []githubsource.Registration{owned}}
}

func FixedTime() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
