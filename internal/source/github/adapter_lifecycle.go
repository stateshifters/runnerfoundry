package github

import (
	"context"
	"fmt"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

type identifiedObservation struct {
	RequestID    int64
	RequestIDSet bool
	MessageID    int64
	EventType    string
	Observation  core.RunnerObservation
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
		if result.Outcome == core.ObservationUnmatched {
			adapter.reportUnmatchedObservation("runner_started", observation)
		}
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
		if result.Outcome == core.ObservationUnmatched {
			adapter.reportUnmatchedObservation("terminalized", observation)
		}
	}
	return nil
}

func (adapter *Adapter) reportObservationFailure(stage string, observation identifiedObservation, err error) {
	activity := ManagerActivity{Visibility: ActivityInfo, Operation: "job_observation", Stage: stage,
		Outcome: "failed", Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
		SourceRequestID: observation.RequestID, SourceRequestIDSet: observation.RequestIDSet,
		RunnerName: observation.Observation.RunnerName,
		MessageID:  observation.MessageID, EventType: observation.EventType, Err: err}
	activity.StateBackend, activity.StateOperation = durableStateFailureContext(err)
	adapter.Observer.report(activity)
}

func (adapter *Adapter) reportUnmatchedObservation(stage string, observation identifiedObservation) {
	adapter.Observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: "job_observation", Stage: stage,
		Outcome: string(core.ObservationUnmatched), Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
		SourceRequestID: observation.RequestID, SourceRequestIDSet: observation.RequestIDSet,
		RunnerName: observation.Observation.RunnerName,
		MessageID:  observation.MessageID, EventType: observation.EventType})
}

func (adapter *Adapter) validateStarted(
	messageID int64,
	events []*upstream.JobStarted,
) ([]identifiedObservation, []ignoredEvidence, error) {
	return validateLifecycleEvents(adapter, messageID, "JobStarted", "validate_runner_started", events,
		func(event *upstream.JobStarted) lifecycleEventMetadata {
			return lifecycleEventMetadata{Base: &event.JobMessageBase, RunnerName: event.RunnerName, RunnerID: event.RunnerID}
		})
}

func (adapter *Adapter) validateCompleted(
	messageID int64,
	events []*upstream.JobCompleted,
) ([]identifiedObservation, []ignoredEvidence, error) {
	return validateLifecycleEvents(adapter, messageID, "JobCompleted", "validate_terminal", events,
		func(event *upstream.JobCompleted) lifecycleEventMetadata {
			return lifecycleEventMetadata{Base: &event.JobMessageBase, RunnerName: event.RunnerName, RunnerID: event.RunnerID}
		})
}

type lifecycleEventMetadata struct {
	Base       *upstream.JobMessageBase
	RunnerName string
	RunnerID   int
}

func validateLifecycleEvents[eventType any](
	adapter *Adapter,
	messageID int64,
	githubEventType string,
	validationStage string,
	events []*eventType,
	metadataFor func(*eventType) lifecycleEventMetadata,
) ([]identifiedObservation, []ignoredEvidence, error) {
	observations := make([]identifiedObservation, 0, len(events))
	ignored := make([]ignoredEvidence, 0)
	for _, event := range events {
		classified, err := classifyLifecycleEvent(
			adapter, messageID, githubEventType, validationStage, event, metadataFor,
		)
		if err != nil {
			return nil, nil, err
		}
		if classified.Evidence != nil {
			ignored = append(ignored, *classified.Evidence)
		}
		if !classified.Actionable {
			continue
		}
		observations = append(observations, classified.Observation)
	}
	return observations, ignored, nil
}

type classifiedLifecycleEvent struct {
	Observation identifiedObservation
	Evidence    *ignoredEvidence
	Actionable  bool
}

func classifyLifecycleEvent[eventType any](
	adapter *Adapter,
	messageID int64,
	githubEventType string,
	validationStage string,
	event *eventType,
	metadataFor func(*eventType) lifecycleEventMetadata,
) (classifiedLifecycleEvent, error) {
	if event == nil {
		evidence := lifecycleEvidence(messageID, githubEventType, validationStage,
			lifecycleEventMetadata{}, "nil_event")
		return classifiedLifecycleEvent{Evidence: &evidence}, nil
	}
	metadata := metadataFor(event)
	if reason := invalidLifecycleIdentityReason(metadata.Base.RunnerRequestID, metadata.RunnerName); reason != "" {
		evidence := lifecycleEvidence(messageID, githubEventType, validationStage, metadata, reason)
		return classifiedLifecycleEvent{Evidence: &evidence}, nil
	}
	observation, err := adapter.lifecycleObservation(metadata.Base.RunnerRequestID, metadata.RunnerName)
	identified := identifiedObservation{RequestID: metadata.Base.RunnerRequestID, MessageID: messageID,
		RequestIDSet: true, EventType: githubEventType,
		Observation: core.RunnerObservation{RunnerName: core.RunnerName(metadata.RunnerName)}}
	if err != nil {
		return classifiedLifecycleEvent{}, adapter.reportInvalidObservation(validationStage, identified, err)
	}
	identified.Observation = observation
	return classifiedLifecycleEvent{Observation: identified, Actionable: true}, nil
}

func invalidLifecycleIdentityReason(requestID int64, runnerName string) string {
	if requestID < 0 {
		return "negative_request_id"
	}
	if requestID == 0 && runnerName == "" {
		return "missing_identity"
	}
	return ""
}

func lifecycleEvidence(
	messageID int64,
	eventType string,
	stage string,
	metadata lifecycleEventMetadata,
	reason string,
) ignoredEvidence {
	evidence := ignoredEvidence{Operation: "job_observation", Stage: stage, EventType: eventType,
		MessageID: messageID, RunnerName: core.RunnerName(metadata.RunnerName),
		GitHubRunnerID: int64(metadata.RunnerID), IdentityReason: reason}
	if metadata.Base != nil {
		evidence.SourceRequestID = metadata.Base.RunnerRequestID
		evidence.SourceRequestIDSet = true
		evidence.GitHubJobID = metadata.Base.JobID
		evidence.WorkflowRunID = metadata.Base.WorkflowRunID
	}
	return evidence
}

func (adapter *Adapter) reportInvalidObservation(stage string, observation identifiedObservation, err error) error {
	adapter.reportObservationFailure(stage, observation, err)
	return markManagerActivityReported(err)
}

func (adapter *Adapter) lifecycleObservation(requestID int64, runnerName string) (core.RunnerObservation, error) {
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
