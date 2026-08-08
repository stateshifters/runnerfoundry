package github

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
)

type identifiedObservation struct {
	RequestID   int64
	MessageID   int64
	EventType   string
	Observation core.RunnerObservation
}

func (observation identifiedObservation) identity() string {
	if observation.RequestID > 0 && observation.Observation.RunnerName != "" {
		return fmt.Sprintf("request %d, runner %q", observation.RequestID, observation.Observation.RunnerName)
	}
	if observation.RequestID > 0 {
		return fmt.Sprintf("request %d", observation.RequestID)
	}
	return fmt.Sprintf("runner %q", observation.Observation.RunnerName)
}

func (adapter *Adapter) persistStarted(ctx context.Context, started []identifiedObservation) error {
	for _, observation := range started {
		result, err := adapter.Handler.RunnerStarted(ctx, observation.Observation)
		if err != nil {
			wrapped := fmt.Errorf("durably record GitHub runner started observation (%s): %w", observation.identity(), err)
			if ctx.Err() != nil {
				return wrapped
			}
			adapter.reportObservationFailure("persist_runner_started", observation, wrapped)
			return markManagerActivityReported(wrapped)
		}
		adapter.reportObservationResult("runner_started", observation, result)
	}
	return nil
}

func (adapter *Adapter) persistCompleted(ctx context.Context, completed []identifiedObservation) error {
	for _, observation := range completed {
		result, err := adapter.Handler.Terminalize(ctx, observation.Observation)
		if err != nil {
			wrapped := fmt.Errorf("durably record GitHub runner completion observation (%s): %w", observation.identity(), err)
			if ctx.Err() != nil {
				return wrapped
			}
			adapter.reportObservationFailure("persist_terminal", observation, wrapped)
			return markManagerActivityReported(wrapped)
		}
		adapter.reportObservationResult("terminalized", observation, result)
	}
	return nil
}

func (adapter *Adapter) reportObservationFailure(stage string, observation identifiedObservation, err error) {
	activity := ManagerActivity{Visibility: ActivityInfo, Operation: "job_observation", Stage: stage,
		Outcome: "failed", Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
		SourceRequestID: observation.RequestID, RunnerName: observation.Observation.RunnerName,
		MessageID: observation.MessageID, EventType: observation.EventType, Err: err}
	activity.StateBackend, activity.StateOperation = durableStateFailureContext(err)
	adapter.Observer.report(activity)
}

func (adapter *Adapter) reportObservationResult(stage string, observation identifiedObservation, result core.ObservationResult) {
	visibility := ActivityDebug
	if result.Outcome == core.ObservationUnmatched {
		visibility = ActivityInfo
	}
	adapter.Observer.report(ManagerActivity{Visibility: visibility, Operation: "job_observation", Stage: stage,
		Outcome: string(result.Outcome), Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
		SourceRequestID: observation.RequestID, RunnerName: observation.Observation.RunnerName,
		MessageID: observation.MessageID, EventType: observation.EventType})
}

func (adapter *Adapter) validateStarted(messageID int64, events []*upstream.JobStarted) ([]identifiedObservation, error) {
	return validateLifecycleEvents(adapter, messageID, "JobStarted", "validate_runner_started", events,
		func(event *upstream.JobStarted) (int64, string) { return event.RunnerRequestID, event.RunnerName })
}

func (adapter *Adapter) validateCompleted(
	messageID int64,
	events []*upstream.JobCompleted,
) ([]identifiedObservation, error) {
	return validateLifecycleEvents(adapter, messageID, "JobCompleted", "validate_terminal", events,
		func(event *upstream.JobCompleted) (int64, string) { return event.RunnerRequestID, event.RunnerName })
}

func validateLifecycleEvents[eventType any](
	adapter *Adapter,
	messageID int64,
	githubEventType string,
	validationStage string,
	events []*eventType,
	identity func(*eventType) (int64, string),
) ([]identifiedObservation, error) {
	observations := make([]identifiedObservation, 0, len(events))
	for _, event := range events {
		if event == nil {
			err := fmt.Errorf("GitHub %s message is nil", githubEventType)
			return nil, adapter.reportInvalidObservation(validationStage,
				identifiedObservation{MessageID: messageID, EventType: githubEventType}, err)
		}
		requestID, runnerName := identity(event)
		observation, err := adapter.lifecycleObservation(requestID, runnerName)
		identified := identifiedObservation{RequestID: requestID, MessageID: messageID,
			EventType: githubEventType, Observation: core.RunnerObservation{RunnerName: core.RunnerName(runnerName)}}
		if err != nil {
			return nil, adapter.reportInvalidObservation(validationStage, identified, err)
		}
		identified.Observation = observation
		observations = append(observations, identified)
	}
	return observations, nil
}

func (adapter *Adapter) reportInvalidObservation(stage string, observation identifiedObservation, err error) error {
	adapter.reportObservationFailure(stage, observation, err)
	return markManagerActivityReported(err)
}

func (adapter *Adapter) lifecycleObservation(requestID int64, runnerName string) (core.RunnerObservation, error) {
	if requestID < 0 {
		return core.RunnerObservation{}, fmt.Errorf("GitHub runner request ID must not be negative: %d", requestID)
	}
	if requestID == 0 && runnerName == "" {
		return core.RunnerObservation{}, errors.New("GitHub lifecycle observation requires a positive request ID or runner name")
	}
	observation := core.RunnerObservation{RunnerName: core.RunnerName(runnerName)}
	if requestID == 0 {
		return observation, nil
	}
	source, err := adapter.sourceReference(requestID)
	if err != nil {
		return core.RunnerObservation{}, err
	}
	observation.Source = source
	return observation, nil
}
