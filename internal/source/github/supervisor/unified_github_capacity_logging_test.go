package supervisor

import (
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestGitHubSourceRuntimeReportsAggregateCapacityOnlyWhenItChanges(t *testing.T) {
	t.Parallel()

	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: &capacityMessageClientFake{finished: make(chan struct{}, 2)}, Repository: "https://github.example/one", RunnerPool: "linux"},
		{Client: &capacityMessageClientFake{finished: make(chan struct{}, 2)}, Repository: "https://github.example/two", RunnerPool: "linux"},
	}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager,
		&sourceCapacityProviderFake{capacities: map[core.RunnerPool]int{"linux": 7}},
		"member", eventHandlerFake{}, managerConfigForAdapters(manager.adapters), func() time.Time { return now })
	var events []githubsource.ManagerActivity
	source.observer.Report = func(activity githubsource.ManagerActivity) { events = append(events, activity) }
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	mustCloseGitHubSource(t, source)

	infoCount, debugCount, aggregate := capacityPublicationCounts(events)
	if aggregate.RunnerPool != "linux" || aggregate.Capacity != 7 || aggregate.Count != 2 {
		t.Fatalf("aggregate capacity event = %#v", aggregate)
	}
	if infoCount != 1 || debugCount != 2 {
		t.Fatalf("capacity events info=%d debug=%d, want 1 and 2", infoCount, debugCount)
	}
}

func capacityPublicationCounts(events []githubsource.ManagerActivity) (infoCount int, debugCount int, aggregate githubsource.ManagerActivity) {
	for _, event := range events {
		if event.Operation != "capacity_publication" || event.Outcome != "published" {
			continue
		}
		switch event.Visibility {
		case githubsource.ActivityInfo:
			infoCount++
			aggregate = event
		case githubsource.ActivityDebug:
			debugCount++
		}
	}
	return infoCount, debugCount, aggregate
}
