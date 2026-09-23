package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestGitHubSourceRuntimeSchedulesDiscoveryFromPreviousCompletion(t *testing.T) {
	t.Parallel()
	assertCompletionRelativeDiscoveryCadence(t, nil, 30*time.Second)
}

func TestGitHubSourceRuntimeSchedulesFailedDiscoveryFromPreviousCompletion(t *testing.T) {
	t.Parallel()
	assertCompletionRelativeDiscoveryCadence(t, errors.New("discovery failed"), 0)
}

func assertCompletionRelativeDiscoveryCadence(t *testing.T, reconcileErr error, harvestDelay time.Duration) {
	t.Helper()
	fixture := newDiscoveryCadenceFixture(reconcileErr)
	completed := fixture.completeInitialDiscovery(t, 2*time.Minute)
	fixture.harvestDiscovery(t, completed, harvestDelay, reconcileErr)
	fixture.assertDiscoveryIdle(t)
	fixture.assertDiscoveryRestartsAfter(t, time.Minute-harvestDelay)
	mustCloseGitHubSource(t, fixture.source)
}

type discoveryCadenceFixture struct {
	now     time.Time
	started chan struct{}
	release chan struct{}
	source  *githubSourceRuntime
}

func newDiscoveryCadenceFixture(reconcileErr error) *discoveryCadenceFixture {
	fixture := &discoveryCadenceFixture{
		now:     time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC),
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	manager := &githubManagerFake{reconcileStarted: fixture.started, reconcileRelease: fixture.release,
		reconcileErr: reconcileErr}
	fixture.source = newGitHubSourceRuntime(manager, &sourceCapacityProviderFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, func() time.Time { return fixture.now })
	return fixture
}

func (fixture *discoveryCadenceFixture) completeInitialDiscovery(
	t *testing.T,
	duration time.Duration,
) githubDiscoveryResult {
	t.Helper()
	if err := fixture.source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	<-fixture.started
	fixture.now = fixture.now.Add(duration)
	close(fixture.release)
	return <-fixture.source.term.discoveryResult
}

func (fixture *discoveryCadenceFixture) harvestDiscovery(
	t *testing.T,
	completed githubDiscoveryResult,
	delay time.Duration,
	wantErr error,
) {
	t.Helper()
	fixture.now = fixture.now.Add(delay)
	fixture.source.term.discoveryResult <- completed
	if wantErr == nil {
		awaitGitHubDiscovery(t, fixture.source)
		return
	}
	awaitGitHubDiscoveryError(t, fixture.source, wantErr)
}

func (fixture *discoveryCadenceFixture) assertDiscoveryIdle(t *testing.T) {
	t.Helper()
	if err := fixture.source.Poll(context.Background(), fixture.now); err != nil {
		t.Fatalf("poll after discovery completion: %v", err)
	}
	if fixture.source.term.discoveryActive {
		t.Fatal("discovery restarted before one completion-relative interval elapsed")
	}
}

func (fixture *discoveryCadenceFixture) assertDiscoveryRestartsAfter(t *testing.T, delay time.Duration) {
	t.Helper()
	fixture.now = fixture.now.Add(delay)
	if err := fixture.source.Poll(context.Background(), fixture.now); err != nil {
		t.Fatalf("poll one interval after discovery completion: %v", err)
	}
	if !fixture.source.term.discoveryActive {
		t.Fatal("discovery did not start one interval after the pass completed")
	}
}
