package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type conflictingRegistrationState struct {
	stateMutex   sync.Mutex
	advisoryLock sync.Mutex
	arrived      sync.WaitGroup
	release      chan struct{}
	genericNames map[string]bool
	mirrorName   string
}

func newConflictingRegistrationState() *conflictingRegistrationState {
	state := &conflictingRegistrationState{release: make(chan struct{}), genericNames: map[string]bool{}}
	state.arrived.Add(2)
	return state
}

func (state *conflictingRegistrationState) connection(runner scaler.RunnerLifecycle) *fakeDatabaseConnection {
	return &fakeDatabaseConnection{beginHook: func() (*databaseTransaction, error) {
		candidate := &conflictingRegistrationTransaction{state: state, runner: runner}
		transaction := &fakeDatabaseTransaction{
			execHook: candidate.exec, queryRowHook: candidate.queryRow,
			commitHook: candidate.commit, rollbackHook: candidate.rollback,
		}
		return transaction.databaseTransaction(), nil
	}}
}

type conflictingRegistrationTransaction struct {
	state            *conflictingRegistrationState
	runner           scaler.RunnerLifecycle
	genericCandidate bool
	mirrorCandidate  bool
	lockHeld         bool
}

func (candidate *conflictingRegistrationTransaction) exec(statement string, _ ...any) (pgconn.CommandTag, error) {
	if strings.Contains(statement, "INSERT INTO scaler_runner_lifecycle") {
		candidate.state.arrived.Done()
		<-candidate.state.release
		candidate.acquireLifecycleLock()
		candidate.genericCandidate = true
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	if strings.Contains(statement, "pg_advisory_xact_lock") {
		candidate.acquireLifecycleLock()
		candidate.state.stateMutex.Lock()
		candidate.mirrorCandidate = candidate.state.mirrorName == ""
		candidate.state.stateMutex.Unlock()
		if candidate.mirrorCandidate {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		}
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (candidate *conflictingRegistrationTransaction) acquireLifecycleLock() {
	if candidate.lockHeld {
		return
	}
	candidate.state.advisoryLock.Lock()
	candidate.lockHeld = true
}

func (candidate *conflictingRegistrationTransaction) queryRow(statement string, _ ...any) databaseRow {
	if strings.Contains(statement, "runner_name <>") {
		return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
			setFakeDestination(destinations[0], candidate.otherMirrorOwnerCount())
		}}}
	}
	if strings.Contains(statement, "FROM scaler_runner_lifecycle") {
		if !candidate.genericCandidate {
			return databaseRow{scanner: fakeRow{err: pgx.ErrNoRows}}
		}
		return databaseRow{scanner: terminalGenericRegistrationRow(candidate.runner)}
	}
	if strings.Contains(statement, "FROM scaler_libvirt_runner_lifecycle") {
		if !candidate.ownsMirrorRepresentation() {
			return databaseRow{scanner: fakeRow{err: pgx.ErrNoRows}}
		}
		return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
			setFakeDestination(destinations[0], candidate.runner.RequestID)
			setFakeDestination(destinations[1], false)
		}}}
	}
	return databaseRow{scanner: fakeRow{}}
}

func terminalGenericRegistrationRow(runner scaler.RunnerLifecycle) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], runner.Provider)
		setFakeDestination(destinations[1], runner.RequestID)
		setFakeDestination(destinations[2], runner.AssignmentID)
		setFakeDestination(destinations[3], runner.Phase)
	}}
}

func (candidate *conflictingRegistrationTransaction) ownsMirrorRepresentation() bool {
	if candidate.mirrorCandidate {
		return true
	}
	candidate.state.stateMutex.Lock()
	defer candidate.state.stateMutex.Unlock()
	return candidate.state.mirrorName == candidate.runner.RunnerName
}

func (candidate *conflictingRegistrationTransaction) otherMirrorOwnerCount() int64 {
	candidate.state.stateMutex.Lock()
	defer candidate.state.stateMutex.Unlock()
	for runnerName := range candidate.state.genericNames {
		if runnerName != candidate.runner.RunnerName {
			return 1
		}
	}
	return 0
}

func (candidate *conflictingRegistrationTransaction) commit() error {
	candidate.state.stateMutex.Lock()
	candidate.state.genericNames[candidate.runner.RunnerName] = true
	if candidate.mirrorCandidate {
		candidate.state.mirrorName = candidate.runner.RunnerName
	}
	candidate.state.stateMutex.Unlock()
	candidate.releaseLock()
	return nil
}

func (candidate *conflictingRegistrationTransaction) rollback() error {
	candidate.releaseLock()
	return nil
}

func (candidate *conflictingRegistrationTransaction) releaseLock() {
	if candidate.lockHeld {
		candidate.state.advisoryLock.Unlock()
		candidate.lockHeld = false
	}
}

func TestConcurrentConflictingClusterRegistrationsSerializeAtLifecycleAdvisoryLock(t *testing.T) {
	state := newConflictingRegistrationState()
	runners := conflictingClusterRegistrationRunners()
	results := runConflictingClusterRegistrations(state, runners)
	state.arrived.Wait()
	close(state.release)
	assertConflictingClusterRegistrationResults(t, state, runners, results)
}

func conflictingClusterRegistrationRunners() []scaler.RunnerLifecycle {
	return []scaler.RunnerLifecycle{
		{NodeID: "node-a", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderLibvirt,
			RunnerName: "linux-a", RequestID: 42, AssignmentID: 1, Phase: scaler.RunnerProvisioning},
		{NodeID: "node-b", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderLibvirt,
			RunnerName: "linux-b", RequestID: 42, AssignmentID: 2, Phase: scaler.RunnerProvisioning},
	}
}

func runConflictingClusterRegistrations(
	state *conflictingRegistrationState,
	runners []scaler.RunnerLifecycle,
) <-chan error {
	results := make(chan error, len(runners))
	for _, runner := range runners {
		go func(candidate scaler.RunnerLifecycle) {
			results <- storeWithConnection(state.connection(candidate)).RegisterRunner(context.Background(), candidate)
		}(runner)
	}
	return results
}

func assertConflictingClusterRegistrationResults(
	t *testing.T,
	state *conflictingRegistrationState,
	runners []scaler.RunnerLifecycle,
	results <-chan error,
) {
	t.Helper()
	var successes, conflicts int
	for range runners {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
			conflicts++
		} else {
			t.Fatalf("concurrent registration error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent results = %d successes, %d conflicts", successes, conflicts)
	}
	if len(state.genericNames) != 1 {
		t.Fatalf("committed generic names = %#v", state.genericNames)
	}
}
