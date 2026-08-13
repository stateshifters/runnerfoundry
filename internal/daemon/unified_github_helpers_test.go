package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func mustOpenGitHubSource(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open GitHub source: %v", err)
	}
	for len(source.opening) != 0 {
		if failures := awaitAllGitHubRegistrationOpenResults(source); len(failures) != 0 {
			t.Fatalf("install GitHub repository sessions: %v", errors.Join(failures...))
		}
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
	for source.term.discoveryActive {
		if failures := awaitGitHubDiscoveryProgress(source); len(failures) != 0 {
			t.Fatalf("harvest GitHub discovery: %v", errors.Join(failures...))
		}
	}
}

func awaitGitHubDiscoveryError(t *testing.T, source *githubSourceRuntime, wantErr error) {
	t.Helper()
	for source.term.discoveryActive {
		failures := awaitGitHubDiscoveryProgress(source)
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
	for len(source.opening) != 0 {
		failures := awaitGitHubRegistrationOpenProgress(source)
		if len(failures) != 0 {
			if !errors.Is(errors.Join(failures...), wantErr) {
				t.Fatalf("repository opening error = %v, want %v", errors.Join(failures...), wantErr)
			}
			return
		}
	}
	t.Fatalf("GitHub repository opening did not report %v", wantErr)
}

func awaitGitHubRegistrationOpenProgress(source *githubSourceRuntime) []error {
	result := <-source.term.openResults
	source.term.openResults <- result
	return source.harvestRegistrationOpenResults(context.Background(), source.term)
}

func awaitAllGitHubRegistrationOpenResults(source *githubSourceRuntime) []error {
	results := make([]githubRegistrationOpenResult, 0, len(source.opening))
	for range len(source.opening) {
		results = append(results, <-source.term.openResults)
	}
	for _, result := range results {
		source.term.openResults <- result
	}
	return source.harvestRegistrationOpenResults(context.Background(), source.term)
}

func awaitGitHubDiscoveryProgress(source *githubSourceRuntime) []error {
	result := <-source.term.discoveryResult
	source.term.discoveryResult <- result
	return source.harvestDiscovery(context.Background(), source.term)
}

func newGitHubPollingTestSource(
	adapters []*githubsource.Adapter,
	capacities *capacityAggregatorFake,
	currentTime func() time.Time,
) *githubSourceRuntime {
	manager := &githubManagerFake{adapters: adapters}
	return newGitHubSourceRuntime(manager, capacities, "member-1", eventHandlerFake{},
		managerConfigForAdapters(adapters), time.Minute, currentTime)
}

func managerConfigForAdapters(adapters []*githubsource.Adapter) githubsource.ManagerConfig {
	seen := make(map[core.RunnerPool]struct{}, len(adapters))
	config := githubsource.ManagerConfig{RunnerPools: make([]githubsource.RunnerPoolConfig, 0, len(adapters))}
	for _, adapter := range adapters {
		if _, exists := seen[adapter.RunnerPool]; exists {
			continue
		}
		seen[adapter.RunnerPool] = struct{}{}
		config.RunnerPools = append(config.RunnerPools, githubsource.RunnerPoolConfig{
			RunnerPool: adapter.RunnerPool, RunnerGroup: "default",
		})
	}
	return config
}
