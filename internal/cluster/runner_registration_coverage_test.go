package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestRunnerRegistrationCoversBeginAndCommitFailures(t *testing.T) {
	runner := testRunnerLifecycle()
	runner.Provider = scaler.RunnerProviderDocker
	failure := errors.New("failed")
	if err := storeWithConnection(&fakeDatabaseConnection{beginErr: failure}).RegisterRunner(
		context.Background(), runner,
	); err == nil {
		t.Fatal("expected registration begin failure")
	}
	transaction := registrationTransaction(
		registrationGenericRow(runner), integerRow(0), fakeRow{err: pgx.ErrNoRows}, integerRow(0),
	)
	transaction.commitErr = failure
	if err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RegisterRunner(
		context.Background(), runner,
	); err == nil {
		t.Fatal("expected registration commit failure")
	}
}

func registrationTransaction(rows ...fakeRow) *fakeDatabaseTransaction {
	return &fakeDatabaseTransaction{rowResults: rows}
}

func registrationGenericRow(runner scaler.RunnerLifecycle) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], runner.Provider)
		setFakeDestination(destinations[1], runner.RequestID)
		setFakeDestination(destinations[2], runner.AssignmentID)
		setFakeDestination(destinations[3], runner.Phase)
	}}
}

func registrationMirrorRow(runner scaler.RunnerLifecycle) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], runner.RequestID)
		setFakeDestination(destinations[1], runner.Phase == scaler.RunnerReapPending)
	}}
}

func TestRunnerRegistrationCoversPostReconciliationReloadFailure(t *testing.T) {
	runner := testRunnerLifecycle()
	transaction := registrationTransaction(
		registrationGenericRow(runner), integerRow(0), registrationMirrorRow(runner), integerRow(0),
		fakeRow{err: errors.New("reload")},
	)
	err := registerRunnerInTransaction(context.Background(), transaction.databaseTransaction(), runner)
	if err == nil {
		t.Fatal("expected post-reconciliation reload failure")
	}
}

func TestEnsureGenericRegistrationCoversMutationFailures(t *testing.T) {
	runner := testRunnerLifecycle()
	ctx := context.Background()
	tests := map[string]*fakeDatabaseTransaction{
		"insert": {execErrors: []error{errors.New("insert")}},
		"reload": {
			execTags:   []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1")},
			rowResults: []fakeRow{{err: errors.New("reload")}},
		},
		"missing assignment": {
			execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 0")},
			rowResults: []fakeRow{
				{err: pgx.ErrNoRows}, integerRow(0), {err: pgx.ErrNoRows}, integerRow(0),
			},
		},
	}
	for name, transaction := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ensureClusterGenericRegistration(
				ctx, transaction.databaseTransaction(), runner, clusterLifecycleRegistrationState{},
			); err == nil {
				t.Fatal("expected generic registration failure")
			}
		})
	}
}

func TestReconciledRegistrationValidationCoversConflicts(t *testing.T) {
	runner := testRunnerLifecycle()
	tests := map[string]clusterLifecycleRegistrationState{
		"invalid generic": {
			genericExists: true, provider: scaler.RunnerProviderDocker,
		},
		"missing mirror": validGenericRegistrationState(runner),
		"stale mirror phase": {
			genericExists: true, provider: runner.Provider, requestID: runner.RequestID,
			assignmentID: runner.AssignmentID, phase: runner.Phase, mirrorExists: true,
		},
	}
	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateReconciledClusterLifecycleRegistration(runner, state); err == nil {
				t.Fatal("expected reconciled registration conflict")
			}
		})
	}
}

func validGenericRegistrationState(runner scaler.RunnerLifecycle) clusterLifecycleRegistrationState {
	return clusterLifecycleRegistrationState{
		genericExists: true, provider: runner.Provider, requestID: runner.RequestID,
		assignmentID: runner.AssignmentID, phase: runner.Phase,
	}
}

func TestRegistrationInsertCoversDatabaseAndInvariantFailures(t *testing.T) {
	runner := testRunnerLifecycle()
	for name, transaction := range map[string]*fakeDatabaseTransaction{
		"database":  {execErrors: []error{errors.New("insert")}},
		"duplicate": {execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 2")}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := insertClusterRunnerLifecycle(
				context.Background(), transaction.databaseTransaction(), runner,
			); err == nil {
				t.Fatal("expected registration insert failure")
			}
		})
	}
}

func TestRegistrationStateLoadingCoversEachRepresentationFailure(t *testing.T) {
	runner := testRunnerLifecycle()
	tests := map[string][]fakeRow{
		"generic": {{err: errors.New("generic")}},
		"mirror": {
			{err: pgx.ErrNoRows}, integerRow(0), {err: errors.New("mirror")},
		},
		"mirror owner": {
			{err: pgx.ErrNoRows}, integerRow(0), {err: pgx.ErrNoRows}, {err: errors.New("owner")},
		},
	}
	for name, rows := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := loadClusterLifecycleRegistrationState(
				context.Background(), registrationTransaction(rows...).databaseTransaction(), runner,
			)
			if err == nil {
				t.Fatal("expected registration state load failure")
			}
		})
	}
}

func TestGenericRegistrationValidationCoversAuthoritativeConflicts(t *testing.T) {
	runner := testRunnerLifecycle()
	base := validGenericRegistrationState(runner)
	tests := map[string]clusterLifecycleRegistrationState{
		"provider": copyRegistrationState(base, func(state *clusterLifecycleRegistrationState) { state.provider = scaler.RunnerProviderDocker }),
		"identity": copyRegistrationState(base, func(state *clusterLifecycleRegistrationState) { state.requestID++ }),
		"pending reap": copyRegistrationState(base, func(state *clusterLifecycleRegistrationState) {
			state.phase = scaler.RunnerReapPending
		}),
	}
	runner.Phase = scaler.RunnerProvisioning
	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateClusterGenericRegistrationState(runner, state); err == nil {
				t.Fatal("expected authoritative registration conflict")
			}
		})
	}
}

func copyRegistrationState(
	state clusterLifecycleRegistrationState,
	mutate func(*clusterLifecycleRegistrationState),
) clusterLifecycleRegistrationState {
	mutate(&state)
	return state
}
