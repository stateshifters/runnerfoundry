//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
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
	harness.sourceRuntime.capacities = &failOnceCapacityStore{delegate: harness.runtime.state.store}

	harness.deliver(&upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(0)})
	assertPublishedCapacity(t, session, 0, "fail-closed capacity read")
	harness.deliver(&upstream.RunnerScaleSetMessage{Statistics: e2eMessageStatistics(0)})
	assertPublishedCapacity(t, session, 1, "capacity read recovery")
}

type failOnceCapacityStore struct {
	delegate statecontract.RunnerPoolCapacityStore
	called   atomic.Bool
}

func (store *failOnceCapacityStore) AvailableRunnerPoolCapacity(
	ctx context.Context,
	runnerPool core.RunnerPool,
	cutoff time.Time,
) (int, error) {
	if store.called.CompareAndSwap(false, true) {
		return 0, errors.New("scripted capacity read failure")
	}
	capacity, err := store.delegate.AvailableRunnerPoolCapacity(ctx, runnerPool, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delegate capacity read: %w", err)
	}
	return capacity, nil
}

var _ statecontract.RunnerPoolCapacityStore = (*failOnceCapacityStore)(nil)
