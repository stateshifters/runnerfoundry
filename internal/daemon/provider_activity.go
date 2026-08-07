package daemon

import (
	"log/slog"

	"github.com/meza/RunnerFoundry/internal/execution"
)

func newProviderObserver(report func(error)) func(execution.ProviderActivity) {
	return func(activity execution.ProviderActivity) {
		event := operationalEvent{
			operation:  "provider_" + activity.Operation,
			stage:      "provider",
			outcome:    activity.Outcome,
			runnerPool: activity.RunnerPool,
			endpoint:   string(activity.EndpointID),
			duration:   activity.Duration,
		}
		if activity.Err != nil {
			event.diagnostic = activity.Err.Error()
		}
		reportProgress(report, slog.LevelDebug, event)
	}
}
