package daemon

import (
	"log/slog"

	runtimeservice "github.com/stateshifters/runnerfoundry/internal/runtime"
)

type providerActivityObserver func(runtimeservice.ProviderActivity, string)

func newProviderObserver(report func(error)) providerActivityObserver {
	return func(activity runtimeservice.ProviderActivity, composeService string) {
		event := operationalEvent{
			operation:      "provider_" + activity.Operation,
			stage:          "provider",
			outcome:        activity.Outcome,
			runnerPool:     activity.RunnerPool,
			jobID:          activity.JobID,
			runnerID:       activity.RunnerID,
			endpoint:       string(activity.EndpointID),
			provider:       string(activity.Provider),
			composeService: composeService,
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

func observeProviderActivity(
	observer providerActivityObserver,
	composeService string,
) func(runtimeservice.ProviderActivity) {
	if observer == nil {
		return nil
	}
	return func(activity runtimeservice.ProviderActivity) {
		observer(activity, composeService)
	}
}
