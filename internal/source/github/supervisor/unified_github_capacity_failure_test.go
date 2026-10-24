package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestGitHubSourceRuntimeReportsPoolCapacityFailureOnce(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("GitHub failed")
	fixture := newCapacityFailureFixture(t, wantErr)
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	mustOpenGitHubSource(t, fixture.source)
	if err := fixture.source.Poll(context.Background(), now); !errors.Is(err, wantErr) {
		t.Fatalf("capacity error = %v", err)
	}
	<-fixture.first.finished
	<-fixture.second.finished
	if fixture.first.maxCapacity != 0 || fixture.second.maxCapacity != 0 {
		t.Fatalf("capacities after read failure = %d and %d, want zero",
			fixture.first.maxCapacity, fixture.second.maxCapacity)
	}
	if len(fixture.capacities.queries) != 1 {
		t.Fatalf("failed capacity queries = %d, want one per Runner Pool", len(fixture.capacities.queries))
	}
	mustCloseGitHubSource(t, fixture.source)
}

func TestGitHubSourceRuntimeReadsNeutralCapacitySnapshots(t *testing.T) {
	source := &githubSourceRuntime{capacityProvider: capacitySnapshotProviderFake{available: 3}}
	available, err := source.availableRunnerPoolCapacity(context.Background(), "linux")
	if err != nil || available != 3 {
		t.Fatalf("availableRunnerPoolCapacity() = (%d, %v)", available, err)
	}
}

func TestGitHubSourceRuntimeReportsNeutralCapacitySnapshotFailure(t *testing.T) {
	wantErr := errors.New("snapshot failed")
	source := &githubSourceRuntime{capacityProvider: capacitySnapshotProviderFake{err: wantErr}}
	if _, err := source.availableRunnerPoolCapacity(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("availableRunnerPoolCapacity() error = %v", err)
	}
}

type capacitySnapshotProviderFake struct {
	available int
	err       error
}

func (provider capacitySnapshotProviderFake) CapacitySnapshot(
	context.Context,
	core.RunnerPool,
) (core.CapacitySnapshot, error) {
	return core.CapacitySnapshot{RunnerPool: "linux", Available: provider.available}, provider.err
}

type capacityFailureFixture struct {
	source     *githubSourceRuntime
	first      *capacityMessageClientFake
	second     *capacityMessageClientFake
	capacities *sourceCapacityProviderFake
}

func newCapacityFailureFixture(t *testing.T, wantErr error) capacityFailureFixture {
	t.Helper()
	messageClient := &capacityMessageClientFake{finished: make(chan struct{}, 1)}
	peerClient := &capacityMessageClientFake{finished: make(chan struct{}, 1)}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: messageClient, SourceEvents: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"},
		{Client: peerClient, SourceEvents: eventHandlerFake{}, Repository: "two", RunnerPool: "linux"},
	}}
	failedCapacities := &sourceCapacityProviderFake{err: wantErr}
	source := newGitHubSourceRuntime(manager, failedCapacities, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Now)
	return capacityFailureFixture{source: source, first: messageClient, second: peerClient, capacities: failedCapacities}
}

func TestGitHubSourceRuntimeReportsCloseFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("GitHub failed")
	manager := &githubManagerFake{closeErr: wantErr, adapters: []*githubsource.Adapter{{
		Client: &capacityMessageClientFake{}, SourceEvents: eventHandlerFake{}, Repository: "one", RunnerPool: "linux",
	}}}
	source := newGitHubSourceRuntime(manager, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), func() time.Time { return time.Time{} })
	mustOpenGitHubSource(t, source)
	if err := source.CloseLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v", err)
	}
}
