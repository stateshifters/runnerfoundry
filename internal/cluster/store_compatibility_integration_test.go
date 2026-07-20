//go:build integration

package cluster

import (
	"errors"
	"testing"
	"time"
)

func TestPostgreSQLCompatibilityUsesOnlyLiveHealthyContributors(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	fixture.publish(t, "serenity", NodeUnhealthy,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Other", MaxRunners: 2, Healthy: true},
	)
	status := findScaleSet(t, mustScaleSets(t, fixture.store, fixture.now, fixture.liveFor), "meza-ubuntu-slim")
	if !status.Compatible || status.EffectiveCapacity != 10 {
		t.Fatalf("unhealthy conflict affected capacity: %#v", status)
	}
	fixture.publish(t, "serenity", NodeHealthy,
		Contribution{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Other", MaxRunners: 2, Healthy: true},
	)
	status = findScaleSet(t, mustScaleSets(t, fixture.store, fixture.now, fixture.liveFor), "meza-ubuntu-slim")
	if status.Compatible || status.EffectiveCapacity != 0 {
		t.Fatalf("live conflict contributed capacity: %#v", status)
	}
	if err := fixture.store.Publish(fixture.ctx, "spock", NodeHealthy, []Contribution{
		{ScaleSet: "meza-ubuntu-slim", RunnerGroup: "Default", MaxRunners: 6, Healthy: true},
	}, fixture.now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	status = findScaleSet(t, mustScaleSets(t, fixture.store, fixture.now.Add(2*time.Minute), fixture.liveFor), "meza-ubuntu-slim")
	if !status.Compatible || status.EffectiveCapacity != 6 {
		t.Fatalf("expired conflict affected capacity: %#v", status)
	}
}

func TestPostgreSQLExplicitWithdrawalRemovesOnlyTheFinalDeclarationAndItsDemand(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	if err := fixture.store.Enqueue(fixture.ctx, Demand{
		ScaleSet: "serenity-only", Repository: "repo", RequestID: 100, QueueTime: fixture.now,
	}); err != nil {
		t.Fatal(err)
	}
	fixture.publish(t, "serenity", NodeHealthy)
	statuses := mustScaleSets(t, fixture.store, fixture.now, fixture.liveFor)
	if findScaleSet(t, statuses, "meza-ubuntu-slim").EffectiveCapacity != 10 {
		t.Fatalf("withdrawal removed another host's contribution: %#v", statuses)
	}
	if hasScaleSet(statuses, "serenity-only") {
		t.Fatalf("final withdrawal retained scale set: %#v", statuses)
	}
	fixture.publish(t, "serenity", NodeHealthy,
		Contribution{ScaleSet: "serenity-only", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
	)
	assignment, err := fixture.store.AssignOldest(fixture.ctx, "serenity-only", fixture.now, fixture.liveFor)
	if !errors.Is(err, ErrNoAssignment) || assignment != nil {
		t.Fatalf("withdrawn scale set retained demand: %#v, %v", assignment, err)
	}
}
