//go:build integration

package cluster

import (
	"context"
	"testing"
	"time"
)

type capacityTestCluster struct {
	store   *Store
	dsn     string
	now     time.Time
	liveFor time.Duration
}

func newCapacityTestCluster(t *testing.T) capacityTestCluster {
	t.Helper()
	store, dsn := postgreSQLTestStore(t)
	fixture := capacityTestCluster{
		store: store, dsn: dsn,
		now: time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC), liveFor: time.Minute,
	}
	fixture.publish(t, "spock", NodeHealthy,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 10, Healthy: true},
		Contribution{ScaleSet: "spock-only", RunnerGroup: "Default", MaxRunners: 3, Healthy: true},
	)
	fixture.publish(t, "serenity", NodeHealthy,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 2, Healthy: true},
		Contribution{ScaleSet: "serenity-only", RunnerGroup: "Default", MaxRunners: 4, Healthy: true},
	)
	return fixture
}

func (fixture capacityTestCluster) publish(t *testing.T, nodeID NodeID, health NodeHealth, contributions ...Contribution) {
	t.Helper()
	if err := fixture.store.Publish(context.Background(), nodeID, health, contributions, fixture.now); err != nil {
		t.Fatal(err)
	}
}
