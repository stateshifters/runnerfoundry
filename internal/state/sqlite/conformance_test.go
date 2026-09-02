package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state/storetest"
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
	case scenario == storetest.DelayedAcceptance:
		return exerciseSQLiteDelayedAcceptanceProtocol(t)
	case strings.HasPrefix(name, "claim/"):
		return exerciseSQLiteClaimProtocol(t, scenario)
	case strings.HasPrefix(name, "retry/"):
		return exerciseSQLiteRetryProtocol(t, scenario)
	case strings.HasPrefix(name, "recovery/"):
		return exerciseSQLiteRecoveryProtocol(t, scenario)
	case strings.HasPrefix(name, "removal/"):
		return exerciseSQLiteRemovalProtocol(t, scenario)
	default:
		return exerciseSQLiteSnapshotProtocol(t)
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

func exerciseSQLiteDelayedAcceptanceProtocol(t *testing.T) storetest.Observation {
	t.Helper()
	processing := storetest.ExpectedProcessing()
	transaction := Transaction{execute: outcomeExecute(1),
		query: func(context.Context, string, ...any) (Rows, error) {
			return rowsFromValues([][]any{sqliteClaimedValues(processing, true, false)}), nil
		}, commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction)})
	if _, err := store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{Source: processing.Job.Source}); err != nil {
		t.Fatalf("mark accepted: %v", err)
	}
	current, err := store.ProcessingByPool(context.Background(), processing.Job.RunnerPool)
	if err != nil || len(current) != 1 {
		t.Fatalf("processing after acceptance = %#v, %v", current, err)
	}
	return storetest.Observation{Accepted: current[0].RunnerAccepted}
}

func exerciseSQLiteRetryProtocol(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	processing := storetest.ExpectedProcessing()
	plain := rowFromValues(sqliteClaimedValues(processing, false, false))
	rows := map[storetest.Scenario][]Row{
		storetest.RetryRequeue:      {plain},
		storetest.RetryMissing:      {{scan: func(...any) error { return sql.ErrNoRows }}},
		storetest.RetryAccepted:     {rowFromValues(sqliteClaimedValues(processing, true, false))},
		storetest.RetryTerminal:     {rowFromValues(sqliteClaimedValues(processing, false, true))},
		storetest.RetryClaimChanged: {plain, plain},
	}[scenario]
	affected := int64(1)
	if scenario == storetest.RetryClaimChanged {
		affected = 0
	}
	transaction := Transaction{queryRow: sqliteRowsInOrder(rows...), execute: outcomeExecute(affected),
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), processing)
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
		storetest.RecoveryAccepted:     rowFromValues(sqliteClaimedValues(processing, true, false)),
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

func exerciseSQLiteSnapshotProtocol(t *testing.T) storetest.Observation {
	t.Helper()
	return exerciseSQLiteCapacity(t)
}

func exerciseSQLiteCapacity(t *testing.T) storetest.Observation {
	t.Helper()
	store := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row { return rowFromValues([]any{5, 2, 1}) }})
	capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", storetest.FixedTime())
	requireSQLiteConformanceSuccess(t, "capacity", err)
	return storetest.Observation{Capacity: capacity}
}

func requireSQLiteConformanceSuccess(t *testing.T, operation string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

func sqliteQueuedValues(job core.Job) []any {
	return []any{job.ID.String(), string(job.Source.Adapter), string(job.Source.ID), string(job.RunnerPool), job.EnqueuedAt.UnixNano()}
}

func sqliteClaimedValues(processing core.ProcessingJob, accepted, terminal bool) []any {
	return []any{processing.Job.ID.String(), string(processing.Job.Source.Adapter), string(processing.Job.Source.ID),
		string(processing.Job.RunnerPool), string(processing.MemberID), string(processing.RunnerName), processing.ClaimedAt.UnixNano(),
		string(processing.InventoryEndpointID), accepted, terminal, ""}
}

func sqliteConformanceClaim() core.ProcessingClaim {
	return core.ProcessingClaim{MemberID: "member-a", AttemptID: core.AttemptID{1}, ClaimedAt: storetest.FixedTime(),
		InventoryEndpointID: "docker:member-a"}
}

func sqliteRowsInOrder(rows ...Row) func(context.Context, string, ...any) Row {
	return func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}
}
