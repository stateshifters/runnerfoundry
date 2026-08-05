package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestTerminalAuthorityCoversInvalidAndConflictingStates(t *testing.T) {
	ctx := context.Background()
	runner := testRunnerLifecycle()
	identity := runner.Identity()
	predicate, err := newClusterRunnerIdentityPredicate(identity, 4)
	if err != nil {
		t.Fatal(err)
	}
	queryFailure := errors.New("query")
	tests := map[string]struct {
		row  fakeRow
		want error
	}{
		"query":          {row: fakeRow{err: queryFailure}, want: queryFailure},
		"duplicate name": {row: terminalAuthorityRow(runner, 2, 0), want: scaler.ErrRunnerIdentityInvariant},
		"legacy conflict": {
			row: terminalAuthorityRow(runner, 0, 1), want: scaler.ErrRunnerLifecycleConsistency,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			transaction := transactionReturningRow(test.row)
			_, err := loadClusterTerminalRunnerAuthority(ctx, transaction, runner, identity, predicate)
			if !errors.Is(err, test.want) {
				t.Fatalf("terminal authority error = %v, want %v", err, test.want)
			}
		})
	}
}

func transactionReturningRow(row fakeRow) *databaseTransaction {
	transaction := &fakeDatabaseTransaction{}
	transaction.queryRowHook = func(string, ...any) databaseRow { return databaseRow{scanner: row} }
	return transaction.databaseTransaction()
}

func TestTerminalProviderValidationCoversDockerOutcomes(t *testing.T) {
	ctx := context.Background()
	authority := testRunnerLifecycle()
	authority.Provider = scaler.RunnerProviderDocker
	if err := validateClusterTerminalProvider(
		ctx, transactionReturningRow(fakeRow{err: errors.New("query")}), authority,
	); err == nil {
		t.Fatal("expected terminal provider query failure")
	}
	if err := validateClusterTerminalProvider(ctx, transactionReturningRow(terminalBooleanRow(false)), authority); err != nil {
		t.Fatalf("Docker terminal provider validation = %v", err)
	}
}

func terminalBooleanRow(value bool) fakeRow {
	return fakeRow{scan: func(destinations ...any) { setFakeDestination(destinations[0], value) }}
}

func TestTerminalMirrorDeletionCoversDuplicateAndInspectionFailures(t *testing.T) {
	ctx := context.Background()
	runner := testRunnerLifecycle()
	identity := runner.Identity()
	duplicate := (&fakeDatabaseTransaction{
		execTags: []pgconn.CommandTag{pgconn.NewCommandTag("DELETE 2")},
	}).databaseTransaction()
	if err := deleteClusterLibvirtRunnerLifecycle(ctx, duplicate, runner, identity); !errors.Is(
		err, scaler.ErrRunnerIdentityInvariant,
	) {
		t.Fatalf("duplicate terminal mirror error = %v", err)
	}

	queryFailure := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("DELETE 0")}}
	queryFailure.queryRowHook = func(string, ...any) databaseRow {
		return databaseRow{scanner: fakeRow{err: errors.New("query")}}
	}
	if err := deleteClusterLibvirtRunnerLifecycle(
		ctx, queryFailure.databaseTransaction(), runner, identity,
	); err == nil {
		t.Fatal("expected terminal mirror inspection failure")
	}

	duplicateOwner := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("DELETE 0")}}
	duplicateOwner.queryRowHook = func(string, ...any) databaseRow {
		return databaseRow{scanner: terminalMirrorConflictRow(0, 2)}
	}
	if err := deleteClusterLibvirtRunnerLifecycle(
		ctx, duplicateOwner.databaseTransaction(), runner, identity,
	); !errors.Is(err, scaler.ErrRunnerIdentityInvariant) {
		t.Fatalf("duplicate terminal owner error = %v", err)
	}
}

func terminalMirrorConflictRow(nameConflicts, requestOwners int64) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], nameConflicts)
		setFakeDestination(destinations[1], requestOwners)
	}}
}

func TestTerminalConfirmationRejectsInvalidInternalIdentity(t *testing.T) {
	err := confirmClusterRunnerReapedInTransaction(
		context.Background(), (&fakeDatabaseTransaction{}).databaseTransaction(), scaler.RunnerLifecycle{},
	)
	if !errors.Is(err, scaler.ErrInvalidRunnerIdentity) {
		t.Fatalf("invalid terminal identity error = %v", err)
	}
}
