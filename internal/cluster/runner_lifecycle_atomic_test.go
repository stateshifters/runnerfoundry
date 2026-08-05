package cluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type hermeticClusterLifecycleState struct {
	mutex         sync.Mutex
	generic       *scaler.RunnerLifecycle
	mirror        *hermeticClusterMirror
	mirrorFailure error
	mirrorOwner   string
	genericOwner  string
}

type hermeticClusterMirror struct {
	requestID     int64
	runnerName    string
	reapRequested bool
}

func (state *hermeticClusterLifecycleState) connection(runner scaler.RunnerLifecycle) *fakeDatabaseConnection {
	return &fakeDatabaseConnection{beginHook: func() (*databaseTransaction, error) {
		state.mutex.Lock()
		candidate := &hermeticClusterLifecycleTransaction{
			state: state, runner: runner,
			generic: cloneRunnerLifecycle(state.generic), mirror: cloneClusterMirror(state.mirror),
		}
		return candidate.databaseTransaction(), nil
	}}
}

type hermeticClusterLifecycleTransaction struct {
	state   *hermeticClusterLifecycleState
	runner  scaler.RunnerLifecycle
	generic *scaler.RunnerLifecycle
	mirror  *hermeticClusterMirror
}

func (candidate *hermeticClusterLifecycleTransaction) databaseTransaction() *databaseTransaction {
	transaction := &fakeDatabaseTransaction{
		execHook: candidate.exec, queryRowHook: candidate.queryRow,
		commitHook: candidate.commit, rollbackHook: candidate.rollback,
	}
	return transaction.databaseTransaction()
}

func (candidate *hermeticClusterLifecycleTransaction) exec(statement string, _ ...any) (pgconn.CommandTag, error) {
	if strings.Contains(statement, "INSERT INTO scaler_runner_lifecycle") {
		return candidate.insertGeneric()
	}
	if strings.Contains(statement, "INSERT INTO scaler_libvirt_runner_lifecycle") {
		return candidate.reconcileMirror()
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (candidate *hermeticClusterLifecycleTransaction) insertGeneric() (pgconn.CommandTag, error) {
	if candidate.generic != nil {
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	runner := candidate.runner
	candidate.generic = &runner
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (candidate *hermeticClusterLifecycleTransaction) reconcileMirror() (pgconn.CommandTag, error) {
	if candidate.state.mirrorFailure != nil {
		return pgconn.CommandTag{}, candidate.state.mirrorFailure
	}
	if candidate.mirror == nil {
		candidate.mirror = &hermeticClusterMirror{
			requestID: candidate.runner.RequestID, runnerName: candidate.runner.RunnerName,
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	if candidate.mirror.requestID > 0 && candidate.mirror.requestID != candidate.runner.RequestID {
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	candidate.mirror.requestID = candidate.runner.RequestID
	candidate.mirror.reapRequested = candidate.runner.Phase == scaler.RunnerReapPending
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (candidate *hermeticClusterLifecycleTransaction) queryRow(statement string, _ ...any) databaseRow {
	if strings.Contains(statement, "runner_name <>") {
		if strings.Contains(statement, "FROM scaler_runner_lifecycle") {
			return lifecycleOwnerCountRow(candidate.state.genericOwner, candidate.runner.RunnerName)
		}
		return lifecycleOwnerCountRow(candidate.state.mirrorOwner, candidate.runner.RunnerName)
	}
	if strings.Contains(statement, "FROM scaler_runner_lifecycle") {
		return candidate.genericRow()
	}
	if strings.Contains(statement, "FROM scaler_libvirt_runner_lifecycle") {
		return candidate.mirrorRow()
	}
	return databaseRow{scanner: fakeRow{}}
}

func (candidate *hermeticClusterLifecycleTransaction) genericRow() databaseRow {
	if candidate.generic == nil {
		return databaseRow{scanner: fakeRow{err: pgx.ErrNoRows}}
	}
	return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], candidate.generic.Provider)
		setFakeDestination(destinations[1], candidate.generic.RequestID)
		setFakeDestination(destinations[2], candidate.generic.AssignmentID)
		setFakeDestination(destinations[3], candidate.generic.Phase)
	}}}
}

func (candidate *hermeticClusterLifecycleTransaction) mirrorRow() databaseRow {
	if candidate.mirror == nil ||
		(candidate.mirror.runnerName != "" && candidate.mirror.runnerName != candidate.runner.RunnerName) {
		return databaseRow{scanner: fakeRow{err: pgx.ErrNoRows}}
	}
	return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], candidate.mirror.requestID)
		setFakeDestination(destinations[1], candidate.mirror.reapRequested)
	}}}
}

func (candidate *hermeticClusterLifecycleTransaction) commit() error {
	candidate.state.generic = candidate.generic
	candidate.state.mirror = candidate.mirror
	candidate.state.mutex.Unlock()
	return nil
}

func (candidate *hermeticClusterLifecycleTransaction) rollback() error {
	candidate.state.mutex.Unlock()
	return nil
}

func setFakeDestination(destination, value any) {
	reflect.ValueOf(destination).Elem().Set(reflect.ValueOf(value))
}

func lifecycleOwnerCountRow(owner, runnerName string) databaseRow {
	count := int64(0)
	if owner != "" && owner != runnerName {
		count = 1
	}
	return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], count)
	}}}
}

func cloneRunnerLifecycle(runner *scaler.RunnerLifecycle) *scaler.RunnerLifecycle {
	if runner == nil {
		return nil
	}
	clone := *runner
	return &clone
}

func cloneClusterMirror(mirror *hermeticClusterMirror) *hermeticClusterMirror {
	if mirror == nil {
		return nil
	}
	clone := *mirror
	return &clone
}

func TestClusterLibvirtRegistrationRejectsConflictingMirrorWithoutCommittingGeneric(t *testing.T) {
	runner := testRunnerLifecycle()
	runner.Phase = scaler.RunnerProvisioning
	state := &hermeticClusterLifecycleState{mirror: &hermeticClusterMirror{requestID: 99}}

	err := storeWithConnection(state.connection(runner)).RegisterRunner(context.Background(), runner)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	if state.generic != nil {
		t.Fatalf("conflicting registration committed generic lifecycle = %#v", state.generic)
	}
	if state.mirror.requestID != 99 {
		t.Fatalf("conflicting mirror request ID = %d, want 99", state.mirror.requestID)
	}
}

func TestClusterLibvirtRegistrationRepairsLifecycleRepresentations(t *testing.T) {
	for _, lifecycleState := range []string{"generic-only", "mirror-only", "stale-mirror", "consistent"} {
		t.Run(lifecycleState, func(t *testing.T) {
			runner := testRunnerLifecycle()
			runner.Phase = scaler.RunnerProvisioning
			state := seededHermeticClusterLifecycleState(lifecycleState, runner)

			if err := storeWithConnection(state.connection(runner)).RegisterRunner(context.Background(), runner); err != nil {
				t.Fatalf("RegisterRunner() error = %v", err)
			}
			assertHermeticClusterLifecycleConsistent(t, state, runner)
		})
	}
}

func TestClusterRegistrationRejectsLegacyLibvirtHistoryForDockerProvider(t *testing.T) {
	runner := testRunnerLifecycle()
	runner.Provider = scaler.RunnerProviderDocker
	runner.Phase = scaler.RunnerProvisioning
	state := seededHermeticClusterLifecycleState("consistent", runner)

	err := storeWithConnection(state.connection(runner)).RegisterRunner(context.Background(), runner)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	if state.generic.Provider != scaler.RunnerProviderDocker || state.mirror == nil {
		t.Fatalf("provider conflict mutated lifecycle state: generic=%#v mirror=%#v", state.generic, state.mirror)
	}
}

func TestConcurrentClusterLibvirtRegistrationRetriesConverge(t *testing.T) {
	runner := testRunnerLifecycle()
	runner.Phase = scaler.RunnerProvisioning
	state := &hermeticClusterLifecycleState{}
	arrived := sync.WaitGroup{}
	arrived.Add(2)
	release := make(chan struct{})
	store := NewStore(Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) {
		arrived.Done()
		<-release
		return state.connection(runner).databaseConnection(), nil
	}
	errorsByAttempt := make(chan error, 2)
	for attempt := 0; attempt < 2; attempt++ {
		go func() { errorsByAttempt <- store.RegisterRunner(context.Background(), runner) }()
	}
	arrived.Wait()
	close(release)
	for attempt := 0; attempt < 2; attempt++ {
		if err := <-errorsByAttempt; err != nil {
			t.Fatalf("concurrent retry error = %v", err)
		}
	}
	assertHermeticClusterLifecycleConsistent(t, state, runner)
}

func seededHermeticClusterLifecycleState(
	lifecycleState string,
	runner scaler.RunnerLifecycle,
) *hermeticClusterLifecycleState {
	state := &hermeticClusterLifecycleState{}
	if lifecycleState != "mirror-only" {
		state.generic = cloneRunnerLifecycle(&runner)
	}
	if lifecycleState != "generic-only" {
		state.mirror = &hermeticClusterMirror{requestID: runner.RequestID, runnerName: runner.RunnerName}
	}
	if lifecycleState == "stale-mirror" {
		state.mirror.reapRequested = true
	}
	return state
}

func assertHermeticClusterLifecycleConsistent(
	t *testing.T,
	state *hermeticClusterLifecycleState,
	runner scaler.RunnerLifecycle,
) {
	t.Helper()
	if state.generic == nil || state.mirror == nil {
		t.Fatalf("lifecycle representations = generic %#v mirror %#v", state.generic, state.mirror)
	}
	if state.generic.Provider != runner.Provider || state.generic.RequestID != runner.RequestID ||
		state.mirror.requestID != runner.RequestID || state.mirror.reapRequested {
		t.Fatalf("lifecycle representations = generic %#v mirror %#v", state.generic, state.mirror)
	}
}

func TestClusterLifecycleTransitionsRollbackPartialPairMutations(t *testing.T) {
	ctx := context.Background()
	now := testRunnerLifecycle().CreatedAt
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}
	runner := testRunnerLifecycle()
	assertClusterLifecycleRollback(t, "identity binding after generic mutation", 2, func(store *Store) error {
		return store.BindRunnerIdentity(ctx, "linux", "repo", identity, 7, now)
	})
	assertClusterLifecycleRollback(t, "completion after generic and mirror mutations", 3, func(store *Store) error {
		return store.CompleteRunnerLifecycleJob(ctx, scaler.JobCompletion{
			ScaleSet: "linux", Repository: "repo", RunnerIdentity: identity,
		}, now)
	})
	assertClusterLifecycleRollback(t, "reap request after generic mutation", 2, func(store *Store) error {
		return store.RequestRunnerReap(ctx, "linux", "repo", identity, "done", now)
	})
	assertClusterLifecycleRollback(t, "terminal reap after generic mutation", 2, func(store *Store) error {
		return store.ConfirmRunnerReaped(ctx, runner)
	})
}

func assertClusterLifecycleRollback(
	t *testing.T,
	testName string,
	failureAt int,
	operation func(*Store) error,
) {
	t.Helper()
	t.Run(testName, func(t *testing.T) {
		state := &hermeticLifecycleMutationState{failureAt: failureAt}
		connection, transaction := state.connection()
		err := operation(storeWithConnection(connection))
		if !errors.Is(err, errHermeticLifecycleMutation) {
			t.Fatalf("transition error = %v, want injected failure", err)
		}
		if state.committedMutations != 0 {
			t.Fatalf("committed partial mutations = %d, want 0", state.committedMutations)
		}
		if transaction.rollbackCalls != 1 {
			t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
		}
	})
}

func TestClusterLifecycleTransitionsRepairMissingAndStaleMirrors(t *testing.T) {
	ctx := context.Background()
	now := testRunnerLifecycle().CreatedAt
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}
	assertClusterTransitionRepairsMirrors(t, "identity binding", func(store *Store) error {
		return store.BindRunnerIdentity(ctx, "linux", "repo", identity, 7, now)
	})
	assertClusterTransitionRepairsMirrors(t, "busy", func(store *Store) error {
		return store.MarkRunnerBusy(ctx, "linux", "repo", identity, 7, now)
	})
	assertClusterTransitionRepairsMirrors(t, "completion", func(store *Store) error {
		return store.CompleteRunnerLifecycleJob(ctx, scaler.JobCompletion{
			ScaleSet: "linux", Repository: "repo", RunnerIdentity: identity,
		}, now)
	})
	assertClusterTransitionRepairsMirrors(t, "reap request", func(store *Store) error {
		return store.RequestRunnerReap(ctx, "linux", "repo", identity, "done", now)
	})
}

func assertClusterTransitionRepairsMirrors(t *testing.T, operationName string, operation func(*Store) error) {
	t.Helper()
	for _, lifecycleState := range []string{"generic-only", "stale-mirror"} {
		t.Run(operationName+"/"+lifecycleState, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{}
			if err := operation(storeWithConnection(&fakeDatabaseConnection{transaction: transaction})); err != nil {
				t.Fatal(err)
			}
			assertClusterMirrorReconciliationExecution(t, transaction.executions)
		})
	}
}

func assertClusterMirrorReconciliationExecution(t *testing.T, executions []fakeDatabaseExecution) {
	t.Helper()
	if len(executions) < 2 ||
		!strings.Contains(executions[1].statement, "INSERT INTO scaler_libvirt_runner_lifecycle") ||
		!strings.Contains(executions[1].statement, "ON CONFLICT") {
		t.Fatalf("mirror reconciliation statement = %#v", executions)
	}
}

func TestClusterTerminalReapAcceptsMissingOrStaleMirror(t *testing.T) {
	runner := testRunnerLifecycle()
	for _, test := range []struct {
		name       string
		mirrorRows int64
		rowResults []fakeRow
	}{
		{name: "missing", mirrorRows: 0, rowResults: []fakeRow{integerCountRow(t, 0)}},
		{name: "stale reap flag", mirrorRows: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			transaction := &fakeDatabaseTransaction{
				execTags: []pgconn.CommandTag{
					pgconn.NewCommandTag("DELETE 1"),
					pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", test.mirrorRows)),
				},
				rowResults: test.rowResults,
			}
			if err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ConfirmRunnerReaped(
				context.Background(), runner,
			); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(transaction.executions[1].statement, "reap_requested") {
				t.Fatalf("terminal mirror deletion retained stale-flag precondition: %s", transaction.executions[1].statement)
			}
		})
	}
}

func TestClusterTerminalReapRejectsConflictingMirrorWithoutCommit(t *testing.T) {
	runner := testRunnerLifecycle()
	transaction := &fakeDatabaseTransaction{
		execTags:   []pgconn.CommandTag{pgconn.NewCommandTag("DELETE 1"), pgconn.NewCommandTag("DELETE 0")},
		rowResults: []fakeRow{integerCountRow(t, 1)},
	}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ConfirmRunnerReaped(
		context.Background(), runner,
	)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("terminal reap error = %v, want lifecycle consistency error", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func TestClusterTerminalReapRejectsRequestOwnedByAnotherRunnerName(t *testing.T) {
	runner := testRunnerLifecycle()
	transaction := &fakeDatabaseTransaction{
		execTags: []pgconn.CommandTag{pgconn.NewCommandTag("DELETE 1"), pgconn.NewCommandTag("DELETE 0")},
		rowResults: []fakeRow{{scan: func(destinations ...any) {
			*destination[int64](t, destinations[0]) = 0
			*destination[int64](t, destinations[1]) = 1
		}}},
	}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ConfirmRunnerReaped(
		context.Background(), runner,
	)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("terminal request ownership error = %v, want lifecycle consistency error", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func TestClusterTerminalReapUsesAuthoritativeProvider(t *testing.T) {
	runner := testRunnerLifecycle()
	transaction := &fakeDatabaseTransaction{}
	transaction.queryRowHook = func(statement string, _ ...any) databaseRow {
		if strings.Contains(statement, "cluster_terminal_lifecycle_authority") {
			authority := runner
			authority.Provider = scaler.RunnerProviderDocker
			return databaseRow{scanner: terminalAuthorityRow(authority, 0, 0)}
		}
		return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
			setFakeDestination(destinations[0], true)
		}}}
	}

	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ConfirmRunnerReaped(
		context.Background(), runner,
	)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("terminal provider conflict = %v, want lifecycle consistency error", err)
	}
	if len(transaction.executions) != 0 || transaction.rollbackCalls != 1 {
		t.Fatalf("provider conflict executions = %#v, rollbacks = %d", transaction.executions, transaction.rollbackCalls)
	}
}

func TestClusterTransitionRejectsLegacyMirrorForDockerProvider(t *testing.T) {
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "docker-42"}
	transaction := &fakeDatabaseTransaction{
		execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1"), pgconn.NewCommandTag("INSERT 0 0")},
		rowResults: []fakeRow{
			{scan: func(destinations ...any) {
				*destination[NodeID](t, destinations[0]) = "node"
				*destination[scaler.RunnerProvider](t, destinations[1]) = scaler.RunnerProviderDocker
				*destination[string](t, destinations[2]) = identity.RunnerName
			}},
			integerCountRow(t, 1),
		},
	}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).BindRunnerIdentity(
		context.Background(), "linux", "repo", identity, 7, testRunnerLifecycle().CreatedAt,
	)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("provider conflict error = %v, want lifecycle consistency error", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}
