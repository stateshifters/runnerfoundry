package daemon

import (
	"log/slog"

	"github.com/meza/RunnerFoundry/internal/execution"
)

func newProviderObserver(report func(error)) func(execution.ProviderActivity) {
	return func(activity execution.ProviderActivity) {
		event := operationalEvent{
			operation:      "provider_" + activity.Operation,
			stage:          "provider",
			outcome:        activity.Outcome,
			runnerPool:     activity.RunnerPool,
			jobID:          activity.JobID,
			runnerID:       activity.RunnerID,
			endpoint:       string(activity.EndpointID),
			provider:       string(activity.Provider),
			composeService: activity.ComposeService,
			duration:       activity.Duration,
			sourcePC:       activity.OriginPC,
		}
		if activity.Err != nil {
			event.diagnostic = activity.Err.Error()
			event.err = activity.Err
			reportOperational(report, event)
			return
		}
		reportProgress(report, slog.LevelDebug, event)
	}
}
