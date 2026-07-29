package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestApplyDiscoveryStartsEveryConfiguredSessionWithoutListingWhenPruningIsDisabled(t *testing.T) {
	fixture := newPartialRegistrationTestFixture()
	fixture.registry.failWindows = false
	fixture.registry.listErr = errors.New("listing must not be required")

	if err := fixture.reconcile(); err != nil {
		t.Fatal(err)
	}
	managed := fixture.active.Repositories[fixture.repositoryURL]
	if len(managed.Lifecycles) != 2 || len(fixture.started) != 2 {
		t.Fatalf("lifecycles=%d started=%#v", len(managed.Lifecycles), fixture.started)
	}
	if fixture.registry.listCalls != 0 {
		t.Fatalf("registration lists=%d, want 0", fixture.registry.listCalls)
	}
}

func TestApplyDiscoveryRetriesDegradedPruningWithoutStoppingConfirmedSessions(t *testing.T) {
	fixture := activeRegistrationAuditFixture(t)
	fixture.active.Config.Prune = true
	fixture.registry.listErr = errors.New("pruning list unavailable")

	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err == nil {
		t.Fatal("expected degraded pruning")
	}
	assertActiveRepositoryIsDegraded(t, fixture)

	fixture.registry.listErr = nil
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err != nil {
		t.Fatal(err)
	}
	assertActiveRepositoryIsOperational(t, fixture)
	if fixture.registry.listCalls != 2 {
		t.Fatalf("pruning lists=%d, want one failed attempt and one retry", fixture.registry.listCalls)
	}
}

func TestRegistrationLookupFailuresRetainHealthySessions(t *testing.T) {
	for name, lookupErr := range map[string]error{
		"service unavailable": errors.New("service unavailable"),
		"timeout":             context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			fixture := activeRegistrationAuditFixture(t)
			fixture.registry.getErr = lookupErr
			if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err == nil {
				t.Fatal("expected degraded registration reconciliation")
			}
			assertActiveRepositoryIsDegraded(t, fixture)
		})
	}
}

func assertActiveRepositoryIsDegraded(t *testing.T, fixture *activeRegistrationAuditTestFixture) {
	t.Helper()
	managed := fixture.active.Repositories[fixture.repositoryURL]
	if fixture.started != 1 || fixture.closed != 0 || len(managed.Lifecycles) != 1 ||
		managed.RegistrationStatus != repositoryRegistrationDegraded {
		t.Fatalf("started=%d closed=%d lifecycles=%d status=%q",
			fixture.started, fixture.closed, len(managed.Lifecycles), managed.RegistrationStatus)
	}
}

func assertActiveRepositoryIsOperational(t *testing.T, fixture *activeRegistrationAuditTestFixture) {
	t.Helper()
	managed := fixture.active.Repositories[fixture.repositoryURL]
	if fixture.started != 1 || fixture.closed != 0 || len(managed.Lifecycles) != 1 ||
		managed.RegistrationStatus != repositoryRegistrationOperational {
		t.Fatalf("started=%d closed=%d lifecycles=%d status=%q",
			fixture.started, fixture.closed, len(managed.Lifecycles), managed.RegistrationStatus)
	}
}
