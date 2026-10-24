package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestQueueRejectsIncompleteDemandBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	databaseError := errors.New("database unavailable")
	store := newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, databaseError
	}})
	result, err := store.Queue(context.Background(), core.JobRequest{})
	if err == nil || errors.Is(err, databaseError) || result.Outcome != "" {
		t.Fatalf("incomplete demand = %+v, %v; want validation rejection", result, err)
	}
}

func TestClaimRequiresCallerOwnedAttemptIdentity(t *testing.T) {
	t.Parallel()
	databaseError := errors.New("database unavailable")
	store := newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, databaseError
	}})
	result, err := store.ClaimOldest(context.Background(), "linux", core.ProcessingClaim{
		MemberID: "member", ClaimedAt: time.Unix(100, 0), InventoryEndpointID: "endpoint",
	})
	if err == nil || errors.Is(err, databaseError) || result.Outcome != "" {
		t.Fatalf("incomplete claim = %+v, %v; want validation rejection", result, err)
	}
}
