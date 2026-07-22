//go:build integration

package cluster

import "testing"

func TestPostgreSQLAggregatesSharedAndDisjointHostCapacity(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	assertScaleSetStatus(t, fixture.store, fixture.now, fixture.liveFor, map[string]ScaleSetStatus{
		"meza-ubuntu-slim": {Name: "meza-ubuntu-slim", RunnerGroup: "Default", EffectiveCapacity: 12, Compatible: true},
		"serenity-only":    {Name: "serenity-only", RunnerGroup: "Default", EffectiveCapacity: 4, Compatible: true},
		"spock-only":       {Name: "spock-only", RunnerGroup: "Default", EffectiveCapacity: 3, Compatible: true},
	})
}
