package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/state/storetest"
)

func TestLifecycleConformance(t *testing.T) {
	storetest.RunConformance(t, sqliteConformance{})
}

type sqliteConformance struct{}

func (sqliteConformance) Exercise(t *testing.T, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	if scenario == storetest.RetryClaimChanged {
		return exerciseSQLiteClaimChanged(t)
	}
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close SQLite store: %v", closeErr)
		}
	})
	return exerciseSQLiteScenario(t, store, scenario)
}

func exerciseSQLiteScenario(t *testing.T, store *Store, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	name := string(scenario)
	switch {
	case strings.HasPrefix(name, "queue/"):
		return exerciseSQLiteQueue(t, store, scenario)
	case scenario == storetest.DelayedStart:
		return exerciseSQLiteDelayedStart(t, store)
	case strings.HasPrefix(name, "claim/"):
		return exerciseSQLiteClaim(t, store, scenario)
	case strings.HasPrefix(name, "retry/"):
		return exerciseSQLiteRetry(t, store, scenario)
	case strings.HasPrefix(name, "recovery/"):
		return exerciseSQLiteRecovery(t, store, scenario)
	case strings.HasPrefix(name, "removal/"):
		return exerciseSQLiteRemoval(t, store, scenario)
	case scenario == storetest.Capacity:
		return exerciseSQLiteCapacity(t, store)
	default:
		return exerciseSQLiteRegistrations(t, store)
	}
}

func exerciseSQLiteQueue(t *testing.T, store *Store, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	request := requestFor(storetest.OldestJob())
	if scenario == storetest.QueueDuplicate || scenario == storetest.QueueProcessing {
		mustQueue(t, store, request)
	}
	if scenario == storetest.QueueProcessing {
		mustClaim(t, store)
	}
	if scenario == storetest.QueueTerminal {
		if err := store.TerminalizeSource(context.Background(), request.Source); err != nil {
			t.Fatalf("terminalize source: %v", err)
		}
	}
	result, err := store.Queue(context.Background(), request)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	return storetest.Observation{QueueOutcome: result.Outcome}
}

func exerciseSQLiteClaim(t *testing.T, store *Store, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	if scenario == storetest.ClaimEmpty {
		result, err := store.ClaimOldest(context.Background(), "linux", conformanceClaim())
		if err != nil {
			t.Fatalf("empty claim: %v", err)
		}
		return storetest.Observation{ClaimOutcome: result.Outcome}
	}
	newer := storetest.OldestJob()
	newer.Source.ID = "newer"
	newer.EnqueuedAt = newer.EnqueuedAt.Add(time.Minute)
	mustQueue(t, store, requestFor(newer))
	mustQueue(t, store, requestFor(storetest.OldestJob()))
	processing := mustClaim(t, store)
	processing.Job.ID = uuid.Nil
	observation := storetest.Observation{ClaimOutcome: core.ClaimClaimed, Job: processing.Job}
	if scenario == storetest.ClaimMetadata {
		observation.Processing = processing
	}
	return observation
}

func exerciseSQLiteDelayedStart(t *testing.T, store *Store) storetest.Observation {
	t.Helper()
	processing := queueAndClaim(t, store)
	if err := store.MarkRunnerStarted(context.Background(), processing.Job.Source); err != nil {
		t.Fatalf("mark runner started: %v", err)
	}
	current, err := store.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(current) != 1 {
		t.Fatalf("processing after start = %#v, %v", current, err)
	}
	return storetest.Observation{Started: current[0].RunnerStarted}
}

func exerciseSQLiteRetry(t *testing.T, store *Store, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	if scenario == storetest.RetryMissing {
		result, err := store.RetryUnstarted(context.Background(), uuid.New())
		if err != nil {
			t.Fatalf("retry missing: %v", err)
		}
		return storetest.Observation{RetryOutcome: result.Outcome}
	}
	processing := queueAndClaim(t, store)
	if scenario == storetest.RetryStarted {
		mustMarkStarted(t, store, processing.Job.Source)
	}
	if scenario == storetest.RetryTerminal {
		mustTerminalize(t, store, processing.Job.Source)
	}
	result, err := store.RetryUnstarted(context.Background(), processing.Job.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exerciseSQLiteClaimChanged(t *testing.T) storetest.Observation {
	t.Helper()
	transaction := Transaction{queryRow: constantRow(rowFromValues(processingOutcomeValues(false, false))),
		execute: outcomeExecute(0), commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID)
	if err != nil {
		t.Fatalf("retry changed claim: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exerciseSQLiteRecovery(t *testing.T, store *Store, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	processing := storetest.ExpectedProcessing()
	if scenario != storetest.RecoveryMissing {
		processing = queueAndClaim(t, store)
	}
	if scenario == storetest.RecoveryMissing {
		processing.Job.ID = uuid.New()
	}
	if scenario == storetest.RecoveryStarted {
		mustMarkStarted(t, store, processing.Job.Source)
	}
	if scenario == storetest.RecoveryTerminal {
		mustTerminalize(t, store, processing.Job.Source)
	}
	if scenario == storetest.RecoveryClaimChanged {
		processing.ClaimedAt = processing.ClaimedAt.Add(time.Second)
	}
	result, err := store.RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	return storetest.Observation{RetryOutcome: result.Outcome}
}

func exerciseSQLiteRemoval(t *testing.T, store *Store, scenario storetest.Scenario) storetest.Observation {
	t.Helper()
	jobID := uuid.New()
	if scenario != storetest.RemovalMissing {
		processing := queueAndClaim(t, store)
		jobID = processing.Job.ID
		if scenario == storetest.RemovalRemoved {
			mustTerminalize(t, store, processing.Job.Source)
		}
	}
	result, err := store.RemoveTerminal(context.Background(), jobID)
	if err != nil {
		t.Fatalf("remove terminal: %v", err)
	}
	return storetest.Observation{RemovalOutcome: result.Outcome}
}

func exerciseSQLiteCapacity(t *testing.T, store *Store) storetest.Observation {
	t.Helper()
	if err := store.PublishMemberState(context.Background(), "member-a", storetest.FixedTime(),
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 5}}); err != nil {
		t.Fatalf("publish capacity: %v", err)
	}
	for index := 0; index < 3; index++ {
		job := storetest.OldestJob()
		job.Source.ID = core.SourceIdentifier(string(rune('a' + index)))
		mustQueue(t, store, requestFor(job))
	}
	mustClaim(t, store)
	capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", storetest.FixedTime())
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}
	return storetest.Observation{Capacity: capacity}
}

func exerciseSQLiteRegistrations(t *testing.T, store *Store) storetest.Observation {
	t.Helper()
	want := storetest.ExpectedRegistrations()
	if err := store.ReplaceRegistrationSnapshot(context.Background(), want); err != nil {
		t.Fatalf("replace registrations: %v", err)
	}
	got, err := store.RegistrationSnapshot(context.Background())
	if err != nil {
		t.Fatalf("read registrations: %v", err)
	}
	return storetest.Observation{Registrations: got}
}

func queueAndClaim(t *testing.T, store *Store) core.ProcessingJob {
	t.Helper()
	mustQueue(t, store, requestFor(storetest.OldestJob()))
	return mustClaim(t, store)
}

func mustQueue(t *testing.T, store *Store, request core.JobRequest) {
	t.Helper()
	if _, err := store.Queue(context.Background(), request); err != nil {
		t.Fatalf("queue fixture: %v", err)
	}
}

func mustClaim(t *testing.T, store *Store) core.ProcessingJob {
	t.Helper()
	if err := store.PublishMemberState(context.Background(), "member-a", storetest.FixedTime(),
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 5}}); err != nil {
		t.Fatalf("publish member fixture: %v", err)
	}
	result, err := store.ClaimOldest(context.Background(), "linux", conformanceClaim())
	if err != nil || result.Outcome != core.ClaimClaimed {
		t.Fatalf("claim fixture = %#v, %v", result, err)
	}
	return result.Processing
}

func conformanceClaim() core.ProcessingClaim {
	return core.ProcessingClaim{MemberID: "member-a", ClaimedAt: storetest.FixedTime(), InventoryEndpointID: "docker:member-a"}
}

func requestFor(job core.Job) core.JobRequest {
	return core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool, EnqueuedAt: job.EnqueuedAt}
}

func mustMarkStarted(t *testing.T, store *Store, source core.SourceReference) {
	t.Helper()
	if err := store.MarkRunnerStarted(context.Background(), source); err != nil {
		t.Fatalf("mark started: %v", err)
	}
}

func mustTerminalize(t *testing.T, store *Store, source core.SourceReference) {
	t.Helper()
	if err := store.TerminalizeSource(context.Background(), source); err != nil {
		t.Fatalf("terminalize: %v", err)
	}
}
