package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestConcurrentClaimsSerializeAndPreserveFIFO(t *testing.T) {
	t.Parallel()
	fake := newConcurrentClaimProtocol()
	store := NewStore(fake.executor())
	claimedAt := time.Date(2026, time.August, 5, 13, 0, 0, 0, time.UTC)

	first := make(chan claimResult, 1)
	go claimInBackground(store, testMemberIDA, claimedAt, first)
	waitForSignal(t, fake.queryReached[0], "first claim query")

	second := make(chan claimResult, 1)
	go claimInBackground(store, testMemberIDB, claimedAt.Add(time.Second), second)
	waitForSignal(t, fake.lockAttempted[1], "second claim lock attempt")
	select {
	case <-fake.queryReached[1]:
		t.Fatal("second claim queried before the first transaction released the pool lock")
	default:
	}

	close(fake.releaseFirst)
	firstResult := waitForClaim(t, first)
	secondResult := waitForClaim(t, second)
	if firstResult.err != nil || secondResult.err != nil || !firstResult.claimed || !secondResult.claimed {
		t.Fatalf("claims = %#v, %#v", firstResult, secondResult)
	}
	if firstResult.record.Job.ID != testJobID1 || secondResult.record.Job.ID != testJobID2 {
		t.Fatalf("claim order = %s, %s; want distinct FIFO jobs %s, %s",
			firstResult.record.Job.ID, secondResult.record.Job.ID, testJobID1, testJobID2)
	}
}

type claimResult struct {
	record  core.JobRecord
	claimed bool
	err     error
}

type concurrentClaimProtocol struct {
	mu            sync.Mutex
	nextJob       int64
	nextTx        int
	poolLock      chan struct{}
	releaseFirst  chan struct{}
	lockAttempted [2]chan struct{}
	queryReached  [2]chan struct{}
}

func newConcurrentClaimProtocol() *concurrentClaimProtocol {
	fake := &concurrentClaimProtocol{
		nextJob:      1,
		poolLock:     make(chan struct{}, 1),
		releaseFirst: make(chan struct{}),
	}
	fake.poolLock <- struct{}{}
	for index := range fake.lockAttempted {
		fake.lockAttempted[index] = make(chan struct{})
		fake.queryReached[index] = make(chan struct{})
	}
	return fake
}

func (fake *concurrentClaimProtocol) executor() Executor {
	return Executor{begin: func(context.Context) (Transaction, error) {
		fake.mu.Lock()
		transactionID := fake.nextTx
		fake.nextTx++
		fake.mu.Unlock()
		if transactionID >= len(fake.lockAttempted) {
			return Transaction{}, fmt.Errorf("unexpected transaction %d", transactionID)
		}
		return fake.transaction(transactionID), nil
	}}
}

func (fake *concurrentClaimProtocol) transaction(transactionID int) Transaction {
	return Transaction{
		execute: func(context.Context, string, ...any) (Result, error) {
			close(fake.lockAttempted[transactionID])
			<-fake.poolLock
			return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
		},
		queryRow: func(_ context.Context, _ string, arguments ...any) Row {
			close(fake.queryReached[transactionID])
			return Row{scan: func(destinations ...any) error {
				if transactionID == 0 {
					<-fake.releaseFirst
				}
				fake.mu.Lock()
				jobNumber := fake.nextJob
				fake.nextJob++
				fake.mu.Unlock()
				values, err := concurrentClaimValues(jobNumber, arguments)
				if err != nil {
					return err
				}
				return assignDestinations(destinations, values)
			}}
		},
		commit: func(context.Context) error {
			fake.poolLock <- struct{}{}
			return nil
		},
		rollback: func(context.Context) error { return nil },
	}
}

func concurrentClaimValues(jobID int64, arguments []any) ([]any, error) {
	if len(arguments) != 3 {
		return nil, fmt.Errorf("claim arguments = %d, want 3", len(arguments))
	}
	member, memberOK := arguments[0].(core.MemberID)
	claimedAt, claimedAtOK := arguments[1].(time.Time)
	runnerPool, runnerPoolOK := arguments[2].(string)
	if !memberOK || !claimedAtOK || !runnerPoolOK {
		return nil, fmt.Errorf(
			"claim argument types = (%T, %T, %T), want (uuid.UUID, time.Time, string)",
			arguments[0], arguments[1], arguments[2],
		)
	}
	jobIdentifier := testJobID1
	if jobID == 2 {
		jobIdentifier = testJobID2
	}
	return []any{jobIdentifier, "github", fmt.Sprintf("request-%d", jobID), runnerPool, "attempting",
		time.Unix(jobID, 0).UTC(), validNullUUID(member),
		sql.NullTime{Time: claimedAt, Valid: true}}, nil
}

func claimInBackground(store *Store, member core.MemberID, claimedAt time.Time, result chan<- claimResult) {
	record, claimed, err := store.ClaimOldest(context.Background(), "linux", member, claimedAt)
	result <- claimResult{record: record, claimed: claimed, err: err}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, _ string) {
	t.Helper()
	<-signal
}

func waitForClaim(t *testing.T, result <-chan claimResult) claimResult {
	t.Helper()
	return <-result
}
