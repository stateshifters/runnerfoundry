package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/state/storetest"
)

func TestLifecycleConformance(t *testing.T) {
	storetest.RunConformance(t, sqliteConformance{})
}

type sqliteConformance struct{}

func (sqliteConformance) Exercise(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	name := string(scenario)
	switch {
	case strings.HasPrefix(name, "queue/"):
		return exerciseSQLiteQueueProtocol(t, scenario)
	case scenario == storetest.DelayedStart:
		return exerciseSQLiteDelayedStartProtocol(t)
	case strings.HasPrefix(name, "claim/"):
		return exerciseSQLiteClaimProtocol(t, scenario)
	case strings.HasPrefix(name, "retry/"):
		return exerciseSQLiteRetryProtocol(t, scenario)
	case strings.HasPrefix(name, "recovery/"):
		return exerciseSQLiteRecoveryProtocol(t, scenario)
	case strings.HasPrefix(name, "removal/"):
		return exerciseSQLiteRemovalProtocol(t, scenario)
	default:
		return exerciseSQLiteSnapshotProtocol(t, scenario)
	}
}

func exerciseSQLiteQueueProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	request := core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool, EnqueuedAt: job.EnqueuedAt}
	affected := int64(0)
	var queryRow func(context.Context, string, ...any) Row
	switch string(scenario) {
	case string(storetest.QueueNew):
		affected = 1
		queryRow = constantRow(rowFromValues(sqliteQueuedValues(job)))
	case string(storetest.QueueDuplicate):
		queryRow = constantRow(rowFromValues(sqliteQueuedValues(job)))
	case string(storetest.QueueProcessing):
		queryRow = queueClassificationRows(core.QueueProcessing)
	default:
		queryRow = queueClassificationRows(core.QueueSourceTerminal)
	}
	transaction := Transaction{execute: outcomeExecute(affected), queryRow: queryRow,
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Queue(context.Background(), request)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	return storetest.Observation{QueueOutcome: result.Outcome}
}

func exerciseSQLiteClaimProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	row := Row{scan: func(...any) error { return sql.ErrNoRows }}
	if scenario != storetest.ClaimEmpty {
		row = rowFromValues(sqliteQueuedValues(job))
	}
	transaction := Transaction{queryRow: constantRow(row), execute: outcomeExecute(1),
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).ClaimOldest(
		context.Background(), "linux", sqliteConformanceClaim(),
	)
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

func exerciseSQLiteDelayedStartProtocol(t *testing.T) storetest.Observation {
	t.Helper()
	processing := storetest.ExpectedProcessing()
	transaction := Transaction{execute: outcomeExecute(1),
		query: func(context.Context, string, ...any) (Rows, error) {
			return rowsFromValues([][]any{sqliteClaimedValues(processing, true, false)}), nil
		}, commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction)})
	if err := store.MarkRunnerStarted(context.Background(), processing.Job.Source); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	current, err := store.ProcessingByPool(context.Background(), processing.Job.RunnerPool)
	if err != nil || len(current) != 1 {
		t.Fatalf("processing after start = %#v, %v", current, err)
	}
	return storetest.Observation{Started: current[0].RunnerStarted}
}

func exerciseSQLiteRetryProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	job := storetest.OldestJob()
	plain := rowFromValues(sqliteProcessingValues(job, false, false))
	rows := map[storetest.Scenario][]Row{
		storetest.RetryRequeue:      {plain},
		storetest.RetryMissing:      {{scan: func(...any) error { return sql.ErrNoRows }}},
		storetest.RetryStarted:      {rowFromValues(sqliteProcessingValues(job, true, false))},
		storetest.RetryTerminal:     {rowFromValues(sqliteProcessingValues(job, false, true))},
		storetest.RetryClaimChanged: {plain, plain},
	}[scenario]
	affected := int64(1)
	if scenario == storetest.RetryClaimChanged {
		affected = 0
	}
	transaction := Transaction{queryRow: sqliteRowsInOrder(rows...), execute: outcomeExecute(affected),
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exerciseSQLiteRecoveryProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	processing := storetest.ExpectedProcessing()
	affected := int64(0)
	classification := map[storetest.Scenario]Row{
		storetest.RecoveryMissing:      {scan: func(...any) error { return sql.ErrNoRows }},
		storetest.RecoveryStarted:      rowFromValues(sqliteClaimedValues(processing, true, false)),
		storetest.RecoveryTerminal:     rowFromValues(sqliteClaimedValues(processing, false, true)),
		storetest.RecoveryClaimChanged: rowFromValues(sqliteClaimedValues(processing, false, false)),
	}[scenario]
	if scenario == storetest.RecoveryRequeue {
		affected = 1
	}
	transaction := Transaction{execute: outcomeExecute(affected), queryRow: constantRow(classification),
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exerciseSQLiteRemovalProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	affected := int64(1)
	classification := rowFromValues([]any{false})
	if scenario != storetest.RemovalRemoved {
		affected = 0
		classification = rowFromValues([]any{scenario == storetest.RemovalNotTerminal})
	}
	transaction := Transaction{execute: outcomeExecute(affected), queryRow: constantRow(classification),
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), storetest.OldestJob().ID)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	return storetest.Observation{RemovalOutcome: result.Outcome}
}

func exerciseSQLiteSnapshotProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
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
	transaction := Transaction{execute: outcomeExecute(1), query: func(context.Context, string, ...any) (Rows, error) {
		queryCalls++
		registrations := want.Active
		if queryCalls == 2 {
			registrations = want.Owned
		}
		return rowsFromValues(sqliteRegistrationValues(registrations)), nil
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

func sqliteQueuedValues(job core.Job) []any {
	return []any{job.ID.String(), string(job.Source.Adapter), string(job.Source.ID), string(job.RunnerPool), job.EnqueuedAt.UnixNano()}
}

func sqliteProcessingValues(job core.Job, started, terminal bool) []any {
	return []any{job.ID.String(), string(job.Source.Adapter), string(job.Source.ID), string(job.RunnerPool),
		"member-a", started, terminal}
}

func sqliteClaimedValues(processing core.ProcessingJob, started, terminal bool) []any {
	return []any{processing.Job.ID.String(), string(processing.Job.Source.Adapter), string(processing.Job.Source.ID),
		string(processing.Job.RunnerPool), string(processing.MemberID), processing.ClaimedAt.UnixNano(),
		string(processing.InventoryEndpointID), started, terminal}
}

func sqliteConformanceClaim() core.ProcessingClaim {
	return core.ProcessingClaim{MemberID: "member-a", ClaimedAt: storetest.FixedTime(), InventoryEndpointID: "docker:member-a"}
}

func sqliteRowsInOrder(rows ...Row) func(context.Context, string, ...any) Row {
	return func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}
}

func sqliteRegistrationValues(registrations []githubsource.Registration) [][]any {
	values := make([][]any, 0, len(registrations))
	for _, registration := range registrations {
		values = append(values, []any{registration.RepositoryURL, string(registration.RunnerPool), int64(registration.InstallationID),
			int64(registration.RunnerGroupID), int64(registration.ScaleSetID)})
	}
	return values
}
