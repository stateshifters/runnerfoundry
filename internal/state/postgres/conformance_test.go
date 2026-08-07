package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/state/storetest"
)

func TestLifecycleConformance(t *testing.T) {
	storetest.RunConformance(t, postgresConformance{})
}

type postgresConformance struct{}

func (postgresConformance) Exercise(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	name := string(scenario)
	switch {
	case strings.HasPrefix(name, "queue/"):
		return exercisePostgresQueue(t, scenario)
	case scenario == storetest.DelayedStart:
		return exercisePostgresDelayedStart(t)
	case strings.HasPrefix(name, "claim/"):
		return exercisePostgresClaim(t, scenario)
	case strings.HasPrefix(name, "retry/"):
		return exercisePostgresRetry(t, scenario)
	case strings.HasPrefix(name, "recovery/"):
		return exercisePostgresRecovery(t, scenario)
	case strings.HasPrefix(name, "removal/"):
		return exercisePostgresRemoval(t, scenario)
	default:
		return exercisePostgresSnapshot(t, scenario)
	}
}

func exercisePostgresQueue(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	request := core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool, EnqueuedAt: job.EnqueuedAt}
	rows := map[storetest.Scenario][]Row{
		storetest.QueueNew:        {rowFromValues(queuedJobValues(job.ID, request))},
		storetest.QueueDuplicate:  {noPostgresRow(), rowFromValues(queuedJobValues(job.ID, request))},
		storetest.QueueProcessing: {noPostgresRow(), noPostgresRow(), rowFromValues([]any{string(core.QueueProcessing)})},
		storetest.QueueTerminal:   {noPostgresRow(), noPostgresRow(), rowFromValues([]any{string(core.QueueSourceTerminal)})},
	}[scenario]
	result, err := NewStore((&recordedTransaction{rows: rows}).executor()).Queue(context.Background(), request)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	return storetest.Observation{QueueOutcome: result.Outcome}
}

func exercisePostgresClaim(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	rows := []Row{noPostgresRow()}
	if scenario != storetest.ClaimEmpty {
		request := core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool, EnqueuedAt: job.EnqueuedAt}
		rows = []Row{rowFromValues(queuedJobValues(job.ID, request)), rowFromValues(queuedJobValues(job.ID, request))}
	}
	result, err := NewStore((&recordedTransaction{rows: rows}).executor()).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{
		MemberID: "member-a", ClaimedAt: storetest.FixedTime(), InventoryEndpointID: "docker:member-a",
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	observation := storetest.Observation{ClaimOutcome: result.Outcome}
	if result.Outcome == core.ClaimClaimed {
		observation.Job = result.Processing.Job
	}
	if scenario == storetest.ClaimMetadata {
		observation.Processing = result.Processing
	}
	return observation
}

func exercisePostgresDelayedStart(t *testing.T) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	protocol := &recordedTransaction{queryRows: rowsFromValues([][]any{
		postgresClaimedOutcomeValues(storetest.ExpectedProcessing(), true, false),
	})}
	store := NewStore(protocol.executor())
	if err := store.MarkRunnerStarted(context.Background(), job.Source); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	processing, err := store.ProcessingByPool(context.Background(), job.RunnerPool)
	if err != nil || len(processing) != 1 {
		t.Fatalf("processing after start = %#v, %v", processing, err)
	}
	return storetest.Observation{Started: processing[0].RunnerStarted}
}

func exercisePostgresRetry(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	plain := rowFromValues(postgresProcessingOutcomeValues(job, false, false))
	rows := map[storetest.Scenario][]Row{
		storetest.RetryRequeue:      {plain, rowFromValues(queuedJobValues(job.ID, core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool}))},
		storetest.RetryMissing:      {noPostgresRow()},
		storetest.RetryStarted:      {rowFromValues(postgresProcessingOutcomeValues(job, true, false))},
		storetest.RetryTerminal:     {rowFromValues(postgresProcessingOutcomeValues(job, false, true))},
		storetest.RetryClaimChanged: {plain, noPostgresRow(), plain},
	}[scenario]
	result, err := NewStore((&recordedTransaction{rows: rows}).executor()).RetryUnstarted(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exercisePostgresRecovery(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	processing := storetest.ExpectedProcessing()
	rows := map[storetest.Scenario][]Row{
		storetest.RecoveryRequeue: {rowFromValues(queuedJobValues(processing.Job.ID, core.JobRequest{Source: processing.Job.Source,
			RunnerPool: processing.Job.RunnerPool, EnqueuedAt: processing.Job.EnqueuedAt}))},
		storetest.RecoveryMissing:      {noPostgresRow(), noPostgresRow()},
		storetest.RecoveryStarted:      {noPostgresRow(), rowFromValues(postgresClaimedOutcomeValues(processing, true, false))},
		storetest.RecoveryTerminal:     {noPostgresRow(), rowFromValues(postgresClaimedOutcomeValues(processing, false, true))},
		storetest.RecoveryClaimChanged: {noPostgresRow(), rowFromValues(postgresClaimedOutcomeValues(processing, false, false))},
	}[scenario]
	result, err := NewStore((&recordedTransaction{rows: rows}).executor()).RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exercisePostgresRemoval(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	affected := int64(1)
	rows := []Row(nil)
	if scenario != storetest.RemovalRemoved {
		affected = 0
		exists := scenario == storetest.RemovalNotTerminal
		rows = []Row{rowFromValues([]any{exists})}
	}
	commits := 0
	result, err := NewStore(Executor{begin: transactionBeginning(postgresOutcomeTransaction(rows, affected, &commits))}).
		RemoveTerminal(context.Background(), storetest.OldestJob().ID)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	return storetest.Observation{RemovalOutcome: result.Outcome}
}

func exercisePostgresSnapshot(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	if scenario == storetest.Capacity {
		store := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row { return rowFromValues([]any{5, 2, 1}) }})
		capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", storetest.FixedTime())
		if err != nil {
			t.Fatalf("capacity: %v", err)
		}
		return storetest.Observation{Capacity: capacity}
	}
	want := storetest.ExpectedRegistrations()
	queryCalls := 0
	transaction := Transaction{execute: successfulExecute, query: func(context.Context, string, ...any) (Rows, error) {
		queryCalls++
		registrations := want.Active
		if queryCalls == 2 {
			registrations = want.Owned
		}
		return rowsFromValues(registrationValues(registrations)), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction)})
	if err := store.ReplaceRegistrationSnapshot(context.Background(), want); err != nil {
		t.Fatalf("replace registrations: %v", err)
	}
	got, err := store.RegistrationSnapshot(context.Background())
	if err != nil {
		t.Fatalf("read registrations: %v", err)
	}
	return storetest.Observation{Registrations: got}
}

func registrationValues(registrations []githubsource.Registration) [][]any {
	values := make([][]any, 0, len(registrations))
	for _, registration := range registrations {
		values = append(values, []any{registration.RepositoryURL, string(registration.RunnerPool), int64(registration.InstallationID),
			int64(registration.RunnerGroupID), int64(registration.ScaleSetID)})
	}
	return values
}
