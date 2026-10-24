package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestQueueRejectsIncompleteDemandBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	databaseError := errors.New("database unavailable")
	store := newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, databaseError
	}})
	result, err := store.Queue(context.Background(), core.JobRequest{})
	if !errors.Is(err, core.ErrInvalidJobRequest) || errors.Is(err, databaseError) || result.Outcome != "" {
		t.Fatalf("incomplete demand = %+v, %v; want validation rejection", result, err)
	}
}

func TestClaimRejectsIncompleteEvidenceBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	databaseError := errors.New("database unavailable")
	store := newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, databaseError
	}})
	result, err := store.ClaimOldest(context.Background(), "linux", core.ProcessingClaim{})
	if !errors.Is(err, core.ErrInvalidProcessingClaim) || errors.Is(err, databaseError) || result.Outcome != "" {
		t.Fatalf("incomplete claim = %+v, %v; want validation rejection", result, err)
	}
}

func TestDemandTransactionQueueRejectsIncompleteDemandBeforeSourceLock(t *testing.T) {
	t.Parallel()
	databaseError := errors.New("database unavailable")
	transaction := demandTransaction{transaction: transactionStub{execute: failingExecute(databaseError)}}
	result, err := transaction.Queue(context.Background(), core.JobRequest{})
	if !errors.Is(err, core.ErrInvalidJobRequest) || errors.Is(err, databaseError) || result.Outcome != "" {
		t.Fatalf("incomplete demand = %+v, %v; want validation rejection", result, err)
	}
}
