package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type concurrentDockerBindingState struct {
	mutex     sync.Mutex
	lock      sync.Mutex
	arrived   sync.WaitGroup
	release   chan struct{}
	boundName string
	lockKeys  []string
}

func newConcurrentDockerBindingState() *concurrentDockerBindingState {
	state := &concurrentDockerBindingState{release: make(chan struct{})}
	state.arrived.Add(2)
	return state
}

type concurrentDockerBindingTransaction struct {
	state    *concurrentDockerBindingState
	identity scaler.RunnerIdentity
	nodeID   NodeID
	bound    bool
	lockHeld bool
}

func (state *concurrentDockerBindingState) connection(
	nodeID NodeID,
	identity scaler.RunnerIdentity,
) *fakeDatabaseConnection {
	return &fakeDatabaseConnection{beginHook: func() (*databaseTransaction, error) {
		candidate := &concurrentDockerBindingTransaction{state: state, identity: identity, nodeID: nodeID}
		transaction := &fakeDatabaseTransaction{
			execHook: candidate.exec, queryRowHook: candidate.queryRow,
			commitHook: candidate.commit, rollbackHook: candidate.rollback,
		}
		return transaction.databaseTransaction(), nil
	}}
}

func (candidate *concurrentDockerBindingTransaction) exec(
	statement string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	if strings.Contains(statement, "SET phase = 'busy'") {
		candidate.state.arrived.Done()
		<-candidate.state.release
		candidate.acquireLock(lockKeyArgument(arguments))
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	if strings.Contains(statement, "SET request_id = $1") {
		candidate.bound = true
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	if strings.Contains(statement, "INSERT INTO scaler_libvirt_runner_lifecycle") {
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func lockKeyArgument(arguments []any) string {
	key, ok := arguments[len(arguments)-1].(string)
	if !ok {
		return ""
	}
	return key
}

func (candidate *concurrentDockerBindingTransaction) acquireLock(key string) {
	candidate.state.lock.Lock()
	candidate.lockHeld = true
	candidate.state.mutex.Lock()
	candidate.state.lockKeys = append(candidate.state.lockKeys, key)
	candidate.state.mutex.Unlock()
}

func (candidate *concurrentDockerBindingTransaction) queryRow(statement string, _ ...any) databaseRow {
	if strings.Contains(statement, "SELECT COUNT(*) FROM scaler_runner_lifecycle") {
		return candidate.bindingCountRow(statement)
	}
	if strings.Contains(statement, "SELECT node_id, provider, runner_name") {
		return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
			setFakeDestination(destinations[0], candidate.nodeID)
			setFakeDestination(destinations[1], scaler.RunnerProviderDocker)
			setFakeDestination(destinations[2], candidate.identity.RunnerName)
		}}}
	}
	return databaseRow{scanner: integerRow(0)}
}

func (candidate *concurrentDockerBindingTransaction) bindingCountRow(statement string) databaseRow {
	if !strings.Contains(statement, "request_id =") {
		return databaseRow{scanner: integerRow(1)}
	}
	candidate.state.mutex.Lock()
	defer candidate.state.mutex.Unlock()
	if candidate.state.boundName != "" {
		return databaseRow{scanner: integerRow(1)}
	}
	return databaseRow{scanner: integerRow(0)}
}

func integerRow(value int64) fakeRow {
	return fakeRow{scan: func(destinations ...any) { setFakeDestination(destinations[0], value) }}
}

func (candidate *concurrentDockerBindingTransaction) commit() error {
	if candidate.bound {
		candidate.state.mutex.Lock()
		candidate.state.boundName = candidate.identity.RunnerName
		candidate.state.mutex.Unlock()
	}
	candidate.releaseLock()
	return nil
}

func (candidate *concurrentDockerBindingTransaction) rollback() error {
	candidate.releaseLock()
	return nil
}

func (candidate *concurrentDockerBindingTransaction) releaseLock() {
	if candidate.lockHeld {
		candidate.state.lock.Unlock()
		candidate.lockHeld = false
	}
}

func TestConcurrentDockerBindingsSerializeBeforeGenericMutation(t *testing.T) {
	state := newConcurrentDockerBindingState()
	identities := []scaler.RunnerIdentity{
		{RequestID: 42, RunnerName: "docker-a"},
		{RequestID: 42, RunnerName: "docker-b"},
	}
	results := make(chan error, 2)
	for index, identity := range identities {
		go func(nodeID NodeID, candidate scaler.RunnerIdentity) {
			store := storeWithConnection(state.connection(nodeID, candidate))
			results <- store.BindRunnerIdentity(
				context.Background(), "linux", "repo", candidate, 7, time.Unix(1, 0).UTC(),
			)
		}(NodeID("node-"+string(rune('a'+index))), identity)
	}
	state.arrived.Wait()
	close(state.release)
	assertConcurrentDockerBindingResults(t, state, results)
}

func assertConcurrentDockerBindingResults(
	t *testing.T,
	state *concurrentDockerBindingState,
	results <-chan error,
) {
	t.Helper()
	var successes, conflicts int
	for range 2 {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, scaler.ErrRunnerIdentityMismatch) {
			conflicts++
		} else {
			t.Fatalf("concurrent Docker binding error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 || len(state.lockKeys) != 2 || state.lockKeys[0] != state.lockKeys[1] {
		t.Fatalf("bindings = %d successes, %d conflicts; lock keys = %#v", successes, conflicts, state.lockKeys)
	}
}
