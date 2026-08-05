package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

func fixedTime() time.Time {
	return time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC)
}

func TestStorePublicReadBehaviors(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 5, 14, 0, 0, 0, time.UTC)
	store := NewStore(successfulBehaviorExecutor(now))
	ctx := context.Background()
	record, found, err := store.Lookup(ctx, testJobID3)
	verifyFoundJob(t, jobLookupResult{record: record, wasFound: found, err: err})
	record, found, err = store.LookupBySource(ctx, core.SourceReference{Adapter: "github", ID: "request-3"})
	verifyFoundJob(t, jobLookupResult{record: record, wasFound: found, err: err})
	records, err := store.AttemptsByMember(ctx, testMemberIDA, "linux")
	assertJobRecords(t, records, err)
	records, err = store.PendingResults(ctx, 10)
	assertJobRecords(t, records, err)
	records, err = store.PendingAcquisitions(ctx, 10)
	assertJobRecords(t, records, err)
	receipt, found, err := store.LookupReceipt(ctx, core.SourceReference{Adapter: "github", ID: "request-3"})
	if err != nil || !found || receipt.Delivery != core.ResultDeliveryReported {
		t.Fatalf("receipt = %#v, %t, %v", receipt, found, err)
	}
}

func TestStorePublicMutationBehaviors(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 5, 14, 0, 0, 0, time.UTC)
	store := NewStore(successfulBehaviorExecutor(now))
	ctx := context.Background()
	if _, operationErr := store.MarkStartedBySource(ctx,
		core.SourceReference{Adapter: "github", ID: "request-3"}, now); operationErr != nil {
		t.Fatalf("mark started: %v", operationErr)
	}
	if _, operationErr := store.ConfirmAcquisition(ctx, testJobID3, now); operationErr != nil {
		t.Fatalf("confirm acquisition: %v", operationErr)
	}
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "cleanup"}
	if _, operationErr := store.SetResultPendingBySource(ctx,
		core.SourceReference{Adapter: "github", ID: "request-3"}, result); operationErr != nil {
		t.Fatalf("set result by source: %v", operationErr)
	}
	if records, operationErr := store.SetMemberAttemptsResultPending(ctx, testMemberIDA, result); operationErr != nil || len(records) != 1 {
		t.Fatalf("member results = %#v, %v", records, operationErr)
	}
}

func TestStorePublicCapacityBehaviors(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	store := NewStore(successfulBehaviorExecutor(now))
	ctx := context.Background()
	if operationErr := store.PublishMemberState(ctx, testMemberIDA, now,
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}}); operationErr != nil {
		t.Fatalf("publish member state: %v", operationErr)
	}
	members, operationErr := store.ListLiveMembers(ctx, now)
	assertMembers(t, members, operationErr)
	members, operationErr = store.ListExpiredMembers(ctx, now)
	assertMembers(t, members, operationErr)
	if count, countErr := store.CountLiveMembers(ctx, now); countErr != nil || count != 1 {
		t.Fatalf("live count = %d, %v", count, countErr)
	}
	if capacity, capacityErr := store.AvailableRunnerPoolCapacity(ctx, "linux", now); capacityErr != nil || capacity != 2 {
		t.Fatalf("capacity = %d, %v", capacity, capacityErr)
	}
}

func TestStorePublicRegistrationBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor(fixedTime()))
	ctx := context.Background()
	registration := statecontract.Registration{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	if operationErr := store.UpsertRegistration(ctx, registration); operationErr != nil {
		t.Fatalf("upsert registration: %v", operationErr)
	}
	actual, found, operationErr := store.Registration(ctx, registration.RepositoryURL, registration.RunnerPool)
	if operationErr != nil || !found || actual != registration {
		t.Fatalf("registration = %#v, %t, %v", actual, found, operationErr)
	}
	registrations, operationErr := store.Registrations(ctx)
	if operationErr != nil || len(registrations) != 1 || registrations[0] != registration {
		t.Fatalf("registrations = %#v, %v", registrations, operationErr)
	}
	if operationErr := store.DeleteRegistration(ctx, registration.RepositoryURL, registration.RunnerPool); operationErr != nil {
		t.Fatalf("delete registration: %v", operationErr)
	}
}

func successfulBehaviorExecutor(now time.Time) Executor {
	jobValues := fullPostgreSQLJobValues(now)
	registrationValues := []any{"https://example.test/repo", "linux", int64(1), int64(2), int64(3)}
	transaction := Transaction{
		execute: successfulExecute, query: successfulBehaviorQuery(jobValues, registrationValues),
		queryRow: successfulBehaviorQueryRow(jobValues, registrationValues),
		commit:   successfulCompletion, rollback: successfulCompletion,
	}
	return Executor{begin: transactionBeginning(transaction), queryRow: transaction.queryRow}
}

func successfulBehaviorQuery(jobValues, registrationValues []any) func(context.Context, string, ...any) (Rows, error) {
	return func(_ context.Context, statement string, _ ...any) (Rows, error) {
		switch statement {
		case listLiveMembersSQL, listExpiredMembersSQL:
			return rowsFromValues([][]any{{testMemberIDA}}), nil
		case registrationsSQL:
			return rowsFromValues([][]any{registrationValues}), nil
		default:
			return rowsFromValues([][]any{jobValues}), nil
		}
	}
}

func successfulBehaviorQueryRow(jobValues, registrationValues []any) func(context.Context, string, ...any) Row {
	return func(_ context.Context, statement string, _ ...any) Row {
		switch statement {
		case countLiveMembersSQL:
			return rowFromValues([]any{1})
		case availableCapacitySQL:
			return rowFromValues([]any{2, 0, 0})
		case registrationSQL:
			return rowFromValues(registrationValues)
		case lookupReceiptSQL:
			return rowFromValues([]any{"github", "request-3", "failure", "service", "reported"})
		default:
			return rowFromValues(jobValues)
		}
	}
}

func fullPostgreSQLJobValues(now time.Time) []any {
	return []any{testJobID3, "github", "request-3", "linux", "attempting", now.Add(-time.Hour),
		validNullUUID(testMemberIDA), sql.NullTime{Time: now.Add(-time.Minute), Valid: true},
		sql.NullTime{Time: now, Valid: true}, sql.NullTime{Time: now.Add(-30 * time.Second), Valid: true},
		sql.NullTime{Time: now.Add(-20 * time.Second), Valid: true},
		sql.NullString{String: "failure", Valid: true}, sql.NullString{String: "service", Valid: true},
		sql.NullString{String: "cleanup", Valid: true}}
}

func rowFromValues(values []any) Row {
	return Row{scan: func(destinations ...any) error { return assignDestinations(destinations, values) }}
}

func rowsFromValues(values [][]any) Rows {
	index := -1
	return Rows{close: func() error { return nil }, err: func() error { return nil },
		next: func() bool { index++; return index < len(values) },
		scan: func(destinations ...any) error { return assignDestinations(destinations, values[index]) }}
}

type jobLookupResult struct {
	record   core.JobRecord
	wasFound bool
	err      error
}

func verifyFoundJob(t *testing.T, result jobLookupResult) {
	t.Helper()
	if result.err != nil || !result.wasFound || result.record.Job.ID != testJobID3 ||
		result.record.AcquiredAt.IsZero() || result.record.Result == nil {
		t.Fatalf("job = %#v, %t, %v", result.record, result.wasFound, result.err)
	}
}

func assertJobRecords(t *testing.T, records []core.JobRecord, err error) {
	t.Helper()
	if err != nil || len(records) != 1 || records[0].Job.ID != testJobID3 {
		t.Fatalf("records = %#v, %v", records, err)
	}
}

func assertMembers(t *testing.T, members []core.MemberID, err error) {
	t.Helper()
	if err != nil || len(members) != 1 || members[0] != testMemberIDA {
		t.Fatalf("members = %#v, %v", members, err)
	}
}

func TestPublicReadBehaviorsReportMissingRecords(t *testing.T) {
	t.Parallel()
	missing := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return sql.ErrNoRows }}
	}})
	if _, found, err := missing.Lookup(context.Background(), testJobID8); err != nil || found {
		t.Fatalf("missing job found=%t err=%v", found, err)
	}
	if _, found, err := missing.LookupReceipt(context.Background(), core.SourceReference{}); err != nil || found {
		t.Fatalf("missing receipt found=%t err=%v", found, err)
	}
	if _, found, err := missing.Registration(context.Background(), "repo", "linux"); err != nil || found {
		t.Fatalf("missing registration found=%t err=%v", found, err)
	}
}

func TestPublicReadBehaviorsReportProtocolErrors(t *testing.T) {
	t.Parallel()
	broken := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return errors.New("protocol") }}
	}})
	if _, _, err := broken.LookupBySource(context.Background(), core.SourceReference{}); err == nil {
		t.Fatal("expected job lookup error")
	}
	if _, _, err := broken.LookupReceipt(context.Background(), core.SourceReference{}); err == nil {
		t.Fatal("expected receipt lookup error")
	}
	if _, _, err := broken.Registration(context.Background(), "repo", "linux"); err == nil {
		t.Fatal("expected registration lookup error")
	}
}
