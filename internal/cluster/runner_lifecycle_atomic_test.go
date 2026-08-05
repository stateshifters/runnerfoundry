package cluster

import (
	"reflect"
	"strings"
	"sync"

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
