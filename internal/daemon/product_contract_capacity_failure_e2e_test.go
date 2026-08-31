//go:build e2e

package daemon

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestProductContractSQLiteCapacityReadFailurePublishesZero(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseCapacityReadFailure(t, config.StateConfig{Path: ":memory:"}, "e2e-sqlite-capacity-failure")
}

func TestProductContractPostgresCapacityReadFailurePublishesZero(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	exerciseCapacityReadFailure(
		t, config.StateConfig{Postgres: &settings}, "e2e-postgres-capacity-failure",
	)
}

func exerciseCapacityReadFailure(t *testing.T, state config.StateConfig, memberName string) {
	t.Helper()
	harness := newE2EHarness(t, state, memberName, false)
	harness.establishSourceSession()
	session := harness.host.sessions["linux"]
	drainIntegers(session.capacities)
	harness.sourceRuntime.capacityProvider = &failOnceCapacityProvider{delegate: harness.sourceRuntime.capacityProvider}

	harness.deliver(&upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(0)})
	assertPublishedCapacity(t, session, 0, "fail-closed capacity read")
	harness.deliver(&upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(0)})
	assertPublishedCapacity(t, session, 1, "capacity read recovery")
}

type failOnceCapacityProvider struct {
	delegate core.CapacitySnapshotProvider
	called   atomic.Bool
}

func (provider *failOnceCapacityProvider) CapacitySnapshot(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (core.CapacitySnapshot, error) {
	if provider.called.CompareAndSwap(false, true) {
		return core.CapacitySnapshot{}, errors.New("scripted capacity read failure")
	}
	return provider.delegate.CapacitySnapshot(ctx, runnerPool)
}

var _ core.CapacitySnapshotProvider = (*failOnceCapacityProvider)(nil)
