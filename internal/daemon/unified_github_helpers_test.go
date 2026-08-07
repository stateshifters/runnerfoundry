package daemon

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func mustOpenGitHubSource(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open GitHub source: %v", err)
	}
	for attempts := 0; len(source.opening) != 0 && attempts < 10_000; attempts++ {
		runtime.Gosched()
		if failures := source.harvestRepositoryOpenResults(context.Background(), source.term); len(failures) != 0 {
			t.Fatalf("install GitHub repository sessions: %v", errors.Join(failures...))
		}
	}
	if len(source.opening) != 0 {
		t.Fatalf("GitHub repository sessions did not open: %#v", source.opening)
	}
	awaitGitHubDiscovery(t, source)
}

func mustPollGitHubSource(t *testing.T, source *githubSourceRuntime, cutoff time.Time) {
	t.Helper()
	if err := source.Poll(context.Background(), cutoff); err != nil {
		t.Fatalf("poll GitHub source: %v", err)
	}
}

func mustCloseGitHubSource(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("close GitHub source: %v", err)
	}
}

func assertPolledCapacity(t *testing.T, capacities <-chan int, want int, repository string) {
	t.Helper()
	if capacity := <-capacities; capacity != want {
		t.Fatalf("%s capacity = %d, want %d", repository, capacity, want)
	}
}

func assertNoPolledCapacity(t *testing.T, capacities <-chan int, repository string) {
	t.Helper()
	select {
	case capacity := <-capacities:
		t.Fatalf("%s was restarted with capacity %d", repository, capacity)
	default:
	}
}

func preserveCompletedPollResult(source *githubSourceRuntime, adapter *githubsource.Adapter) {
	attempt := source.polls[adapter]
	result := <-attempt.result
	attempt.result <- result
}

func awaitGitHubDiscovery(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	for attempts := 0; source.term.discoveryActive && attempts < 10_000; attempts++ {
		runtime.Gosched()
		if failures := source.harvestDiscovery(context.Background(), source.term); len(failures) != 0 {
			t.Fatalf("harvest GitHub discovery: %v", errors.Join(failures...))
		}
	}
	if source.term.discoveryActive {
		t.Fatal("GitHub discovery did not complete")
	}
}

func awaitGitHubDiscoveryError(t *testing.T, source *githubSourceRuntime, wantErr error) {
	t.Helper()
	for attempts := 0; attempts < 10_000; attempts++ {
		runtime.Gosched()
		failures := source.harvestDiscovery(context.Background(), source.term)
		if len(failures) != 0 {
			if !errors.Is(errors.Join(failures...), wantErr) {
				t.Fatalf("discovery error = %v, want %v", errors.Join(failures...), wantErr)
			}
			return
		}
	}
	t.Fatalf("GitHub discovery did not report %v", wantErr)
}

func awaitGitHubOpeningError(t *testing.T, source *githubSourceRuntime, wantErr error) {
	t.Helper()
	for attempts := 0; attempts < 10_000; attempts++ {
		runtime.Gosched()
		failures := source.harvestRepositoryOpenResults(context.Background(), source.term)
		if len(failures) != 0 {
			if !errors.Is(errors.Join(failures...), wantErr) {
				t.Fatalf("repository opening error = %v, want %v", errors.Join(failures...), wantErr)
			}
			return
		}
	}
	t.Fatalf("GitHub repository opening did not report %v", wantErr)
}

func newGitHubPollingTestSource(
	adapters []*githubsource.Adapter,
	capacities *capacityAggregatorFake,
	currentTime func() time.Time,
) *githubSourceRuntime {
	manager := &githubManagerFake{adapters: adapters}
	return newGitHubSourceRuntime(manager, capacities, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, currentTime)
}
