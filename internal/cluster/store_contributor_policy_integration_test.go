//go:build integration

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgreSQLScaleSetCapacityAndAssignmentAgreeOnContributorEligibility(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	liveFor := time.Minute
	publishContributorPolicyFixtures(t, store, now, liveFor)
	assertContributorPolicy(t, store, now, liveFor, contributorPolicyExpectations())
}

type contributorPublication struct {
	nodeID        NodeID
	health        NodeHealth
	publishedAt   time.Time
	contributions []Contribution
}

type contributorPolicyExpectation struct {
	scaleSet          string
	compatible        bool
	effectiveCapacity int
	assignedNodeID    NodeID
}

func publishContributorPolicyFixtures(t *testing.T, store *Store, now time.Time, liveFor time.Duration) {
	t.Helper()
	publications := append(eligibleContributorPublications(now), excludedContributorPublications(now, liveFor)...)
	for _, publication := range publications {
		if err := store.Publish(context.Background(), publication.nodeID, publication.health, publication.contributions, publication.publishedAt); err != nil {
			t.Fatal(err)
		}
	}
}

func eligibleContributorPublications(now time.Time) []contributorPublication {
	return []contributorPublication{
		{nodeID: "eligible-node", health: NodeHealthy, publishedAt: now, contributions: []Contribution{
			{ScaleSet: "eligible", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
			{ScaleSet: "ignored-conflicts", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
		}},
	}
}

func excludedContributorPublications(now time.Time, liveFor time.Duration) []contributorPublication {
	return []contributorPublication{
		{nodeID: "unhealthy-node", health: NodeUnhealthy, publishedAt: now, contributions: []Contribution{
			{ScaleSet: "unhealthy-node", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
			{ScaleSet: "ignored-conflicts", RunnerGroup: "Other", MaxRunners: 1, Healthy: true},
		}},
		{nodeID: "unhealthy-contribution-node", health: NodeHealthy, publishedAt: now, contributions: []Contribution{
			{ScaleSet: "unhealthy-contribution", RunnerGroup: "Default", MaxRunners: 1, Healthy: false},
			{ScaleSet: "ignored-conflicts", RunnerGroup: "Other", MaxRunners: 1, Healthy: false},
		}},
		{nodeID: "expired-node", health: NodeHealthy, publishedAt: now.Add(-liveFor - time.Second), contributions: []Contribution{
			{ScaleSet: "expired", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
			{ScaleSet: "ignored-conflicts", RunnerGroup: "Other", MaxRunners: 1, Healthy: true},
		}},
		{nodeID: "conflict-default-node", health: NodeHealthy, publishedAt: now, contributions: []Contribution{
			{ScaleSet: "incompatible", RunnerGroup: "Default", MaxRunners: 1, Healthy: true},
		}},
		{nodeID: "conflict-other-node", health: NodeHealthy, publishedAt: now, contributions: []Contribution{
			{ScaleSet: "incompatible", RunnerGroup: "Other", MaxRunners: 1, Healthy: true},
		}},
	}
}

func contributorPolicyExpectations() []contributorPolicyExpectation {
	return []contributorPolicyExpectation{
		{scaleSet: "eligible", compatible: true, effectiveCapacity: 1, assignedNodeID: "eligible-node"},
		{scaleSet: "unhealthy-node", compatible: true},
		{scaleSet: "unhealthy-contribution", compatible: true},
		{scaleSet: "expired", compatible: true},
		{scaleSet: "ignored-conflicts", compatible: true, effectiveCapacity: 1, assignedNodeID: "eligible-node"},
		{scaleSet: "incompatible", compatible: false},
	}
}

func assertContributorPolicy(
	t *testing.T, store *Store, now time.Time, liveFor time.Duration, expectations []contributorPolicyExpectation,
) {
	t.Helper()
	statuses := mustScaleSets(t, store, now, liveFor)
	for expectationIndex, expectation := range expectations {
		assertContributorStatus(t, statuses, expectation)
		assertContributorAssignment(t, store, now, liveFor, expectationIndex, expectation)
	}
}

func assertContributorStatus(t *testing.T, statuses []ScaleSetStatus, expectation contributorPolicyExpectation) {
	t.Helper()
	status := findScaleSet(t, statuses, expectation.scaleSet)
	if status.Compatible != expectation.compatible || status.EffectiveCapacity != expectation.effectiveCapacity {
		t.Fatalf("status %q = %#v, want compatible=%t capacity=%d", expectation.scaleSet, status, expectation.compatible, expectation.effectiveCapacity)
	}
}

func assertContributorAssignment(
	t *testing.T, store *Store, now time.Time, liveFor time.Duration,
	expectationIndex int, expectation contributorPolicyExpectation,
) {
	t.Helper()
	if err := store.Enqueue(context.Background(), Demand{
		ScaleSet: expectation.scaleSet, Repository: "owner/repository",
		RequestID: int64(expectationIndex + 1), QueueTime: now.Add(time.Duration(expectationIndex) * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	assignment, err := store.AssignOldest(context.Background(), expectation.scaleSet, now, liveFor)
	if expectation.assignedNodeID == "" {
		if !errors.Is(err, ErrNoAssignment) || assignment != nil {
			t.Fatalf("assignment for ineligible scale set %q = %#v, %v", expectation.scaleSet, assignment, err)
		}
		return
	}
	if err != nil || assignment == nil || assignment.NodeID != expectation.assignedNodeID {
		t.Fatalf("assignment for eligible scale set %q = %#v, %v; want node %q", expectation.scaleSet, assignment, err, expectation.assignedNodeID)
	}
}
