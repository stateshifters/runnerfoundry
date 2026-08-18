//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"
)

func TestProductContractPostgresForeignRecoveryRetainsUnavailableExactEndpoint(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	first, original := createForeignRecoveryCandidate(t)
	second := newForeignRecoveryMember(t, first)
	if second.provider.EndpointID() == first.provider.EndpointID() {
		t.Fatalf("distinct members share provider endpoint %q by default", second.provider.EndpointID())
	}
	originalOwner := first.runtime.state.member
	prepareForeignRecoveryMember(t, first, second)
	expireForeignOwnerCapacity(t, first, second)
	first.close()
	second.establishSourceSession()
	second.advanceClock(5*time.Minute + time.Second)
	second.completeMaintenanceCycle(second.execution)

	processing, err := second.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	if err != nil || len(processing) != 1 || processing[0].Job.ID != original.JobID ||
		processing[0].MemberID != originalOwner ||
		processing[0].InventoryEndpointID != original.EndpointID {
		t.Fatalf("processing for unavailable exact endpoint = %+v, error = %v", processing, err)
	}
	if second.provider.destroyCount() != 0 || second.provider.attemptCount() != 0 {
		t.Fatalf("unavailable endpoint caused destroys=%d attempts=%d, want no mutation",
			second.provider.destroyCount(), second.provider.attemptCount())
	}
}
