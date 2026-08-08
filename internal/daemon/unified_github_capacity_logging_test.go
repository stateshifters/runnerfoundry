package daemon

import (
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestGitHubSourceRuntimeReportsAggregateCapacityOnlyWhenItChanges(t *testing.T) {
	t.Parallel()

	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: &capacityMessageClientFake{finished: make(chan struct{}, 2)}, Repository: "https://github.example/one", RunnerPool: "linux"},
		{Client: &capacityMessageClientFake{finished: make(chan struct{}, 2)}, Repository: "https://github.example/two", RunnerPool: "linux"},
	}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager,
		&capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}},
		"member", eventHandlerFake{}, githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	var events []progressEventEnvelope
	source.report = func(err error) {
		var event progressEventEnvelope
		if errors.As(err, &event) {
			events = append(events, event)
		}
	}
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	mustCloseGitHubSource(t, source)

	infoCount, debugCount, aggregate := capacityPublicationCounts(events)
	if aggregate.runnerPool != "linux" || aggregate.capacity != 7 || aggregate.count != 2 {
		t.Fatalf("aggregate capacity event = %#v", aggregate)
	}
	if infoCount != 1 || debugCount != 2 {
		t.Fatalf("capacity events info=%d debug=%d, want 1 and 2", infoCount, debugCount)
	}
}

func capacityPublicationCounts(events []progressEventEnvelope) (infoCount int, debugCount int, aggregate operationalEvent) {
	for _, envelope := range events {
		if envelope.event.operation != "capacity_publication" || envelope.event.outcome != "published" {
			continue
		}
		if envelope.level == 0 {
			infoCount++
			aggregate = envelope.event
		} else if envelope.level < 0 {
			debugCount++
		}
	}
	return infoCount, debugCount, aggregate
}
