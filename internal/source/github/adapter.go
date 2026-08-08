// Package github translates GitHub Actions runner scale-set messages at the
// source adapter boundary.
package github

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

const AdapterName = "github"

// ErrNoMessage identifies a successful receive operation with no GitHub
// message available.
var ErrNoMessage = transport.ErrNoMessage

// MessageClient is the GitHub scale-set message transport used by Adapter.
// DeleteMessage is the acknowledgement operation and must permanently remove
// a message only after all translated events have been durably handled.
type MessageClient interface {
	GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error)
	AcquireRequests(ctx context.Context, requestIDs []int64) ([]int64, error)
	DeleteMessage(ctx context.Context, messageID int) error
}

// EventHandler durably accepts source-neutral events translated from GitHub.
// A method must return nil only after its event can survive process failure;
// Adapter acknowledges the containing GitHub message only after every method
// invoked for that message succeeds. Implementations must handle redelivery
// idempotently because acknowledgement can fail after durable handling.
type EventHandler interface {
	Request(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error)
	RunnerStarted(ctx context.Context, observation core.RunnerObservation) (core.ObservationResult, error)
	Terminalize(ctx context.Context, observation core.RunnerObservation) (core.ObservationResult, error)
}

// Adapter receives GitHub scale-set messages and translates their job events
// into source-neutral core requests and observations. Statistics and assigned
// counts are capacity observations only and never create job demand.
//
// PollOnce is not safe for concurrent calls on the same Adapter because the
// acknowledgement cursor is mutable. A failed handler or acknowledgement
// leaves LastMessageID unchanged so GitHub can redeliver the message.
type Adapter struct {
	Client         MessageClient
	Handler        EventHandler
	Observer       ManagerObserver
	Repository     string
	RunnerPool     core.RunnerPool
	InstallationID int64
	RunnerGroupID  int64
	ScaleSetID     int64
	LastMessageID  int
}

// PollResult distinguishes receipt, durable handling, and acknowledgement.
// A false Acknowledged value means the message remains eligible for redelivery.
type PollResult struct {
	Received     bool
	Handled      bool
	Acknowledged bool
}

// PollOnce receives and processes at most one GitHub message. It acknowledges
// the message only after every translated event has been durably handled. The
// returned boolean reports whether GitHub supplied a message, including when
// durable handling or acknowledgement subsequently fails.
func (adapter *Adapter) PollOnce(ctx context.Context, maxCapacity int) (bool, error) {
	result, err := adapter.Poll(ctx, maxCapacity)
	return result.Received, err
}

// Poll receives at most one message and advances each result flag only after
// that protocol step succeeds. Calls on one Adapter must be serialized.
func (adapter *Adapter) Poll(ctx context.Context, maxCapacity int) (PollResult, error) {
	pollStarted := adapter.Observer.currentTime()
	adapter.reportPoll("poll", "started", 0, pollStarted, nil)
	if err := adapter.validatePollBoundaries(); err != nil {
		adapter.reportPoll("poll_configuration", "failed", 0, pollStarted, err)
		return PollResult{}, err
	}
	message, empty, err := adapter.receiveMessage(ctx, maxCapacity, pollStarted)
	if err != nil {
		return PollResult{}, err
	}
	if empty {
		return PollResult{}, nil
	}
	return adapter.handleReceivedMessage(ctx, message, pollStarted)
}

func (adapter *Adapter) handleReceivedMessage(
	ctx context.Context,
	message *upstream.RunnerScaleSetMessage,
	pollStarted time.Time,
) (PollResult, error) {
	result := PollResult{Received: true}
	if err := adapter.requireActivePoll(ctx, "handle_message", message.MessageID, pollStarted); err != nil {
		return result, fmt.Errorf("before message handling: %w", err)
	}
	if err := adapter.handleMessage(ctx, message); err != nil {
		return result, adapter.classifyMessageHandlingFailure(ctx, message.MessageID, pollStarted, err)
	}
	result.Handled = true
	if err := adapter.requireActivePoll(ctx, "acknowledge_message", message.MessageID, pollStarted); err != nil {
		return result, fmt.Errorf("before message acknowledgement: %w", err)
	}
	if err := adapter.acknowledgeMessage(ctx, message.MessageID, pollStarted); err != nil {
		return result, err
	}
	adapter.LastMessageID = message.MessageID
	result.Acknowledged = true
	adapter.reportPoll("poll", "completed", 0, pollStarted, nil)
	return result, nil
}

func (adapter *Adapter) requireActivePoll(
	ctx context.Context,
	stage string,
	messageID int,
	pollStarted time.Time,
) error {
	if err := ctx.Err(); err != nil {
		adapter.reportPoll(stage, "canceled", int64(messageID), pollStarted, nil)
		return fmt.Errorf("GitHub poll canceled: %w", err)
	}
	return nil
}

func (adapter *Adapter) classifyMessageHandlingFailure(
	ctx context.Context,
	messageID int,
	pollStarted time.Time,
	err error,
) error {
	if cancellation := ctx.Err(); cancellation != nil {
		adapter.reportPoll("handle_message", "canceled", int64(messageID), pollStarted, nil)
		return fmt.Errorf("GitHub poll canceled while handling message %d: %w", messageID, err)
	}
	if !managerActivityWasReported(err) {
		adapter.reportPoll("handle_message", "failed", int64(messageID), pollStarted, err)
	}
	return err
}

func (adapter *Adapter) validatePollBoundaries() error {
	if adapter.Client == nil {
		return errors.New("GitHub message client is required")
	}
	if adapter.Handler == nil {
		return errors.New("GitHub event handler is required")
	}
	return nil
}

func (adapter *Adapter) receiveMessage(ctx context.Context, maxCapacity int, started time.Time) (*upstream.RunnerScaleSetMessage, bool, error) {
	message, err := adapter.Client.GetMessage(ctx, adapter.LastMessageID, maxCapacity)
	if errors.Is(err, ErrNoMessage) {
		adapter.reportPoll("poll", "empty", 0, started, nil)
		return nil, true, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			adapter.reportPoll("poll", "canceled", 0, started, nil)
			return nil, false, fmt.Errorf("GitHub poll canceled: %w", err)
		}
		adapter.reportPoll("poll", "failed", 0, started, err)
		return nil, false, fmt.Errorf("receive GitHub scale-set message: %w", err)
	}
	if message == nil {
		return nil, false, errors.New("GitHub message client returned nil without ErrNoMessage")
	}
	return message, false, nil
}

func (adapter *Adapter) acknowledgeMessage(ctx context.Context, messageID int, started time.Time) error {
	if err := adapter.Client.DeleteMessage(ctx, messageID); err != nil {
		if ctx.Err() != nil {
			adapter.reportPoll("acknowledge_message", "canceled", int64(messageID), started, nil)
			return fmt.Errorf("GitHub poll canceled while acknowledging message %d: %w", messageID, err)
		}
		adapter.reportPoll("acknowledge_message", "failed", int64(messageID), started, err)
		return fmt.Errorf("acknowledge GitHub scale-set message %d: %w", messageID, err)
	}
	adapter.reportPoll("acknowledge_message", "completed", int64(messageID), started, nil)
	return nil
}

func (adapter *Adapter) reportPoll(stage, outcome string, messageID int64, started time.Time, err error) {
	activity := ManagerActivity{Visibility: ActivityDebug, Operation: "source_poll", Stage: stage, Outcome: outcome,
		Repository: adapter.Repository, RunnerPool: adapter.RunnerPool, InstallationID: adapter.InstallationID,
		RunnerGroupID: adapter.RunnerGroupID, ScaleSetID: adapter.ScaleSetID, MessageID: messageID, Err: err}
	if outcome != "started" {
		activity.Duration = adapter.Observer.currentTime().Sub(started)
	}
	adapter.Observer.report(activity)
}

func (adapter *Adapter) handleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	effects, err := adapter.validateMessage(message)
	if err != nil {
		return err
	}
	adapter.reportIgnoredEvidence(effects.Ignored)
	acquired, err := adapter.acquireAvailable(ctx, effects)
	if err != nil {
		return err
	}
	if err := adapter.persistRequests(ctx, acquired); err != nil {
		return err
	}
	if err := adapter.persistRequests(ctx, effects.Assigned); err != nil {
		return err
	}
	adapter.reportMessageCount("job_ingestion", "enqueue_jobs", len(acquired)+len(effects.Assigned))
	if err := adapter.persistStarted(ctx, effects.Started); err != nil {
		return err
	}
	adapter.reportMessageCount("job_observation", "runner_started", len(effects.Started))
	if err := adapter.persistCompleted(ctx, effects.Completed); err != nil {
		return err
	}
	adapter.reportMessageCount("job_observation", "terminalized", len(effects.Completed))
	return nil
}

func (adapter *Adapter) reportIgnoredEvidence(evidence []ignoredEvidence) {
	for _, ignored := range evidence {
		adapter.Observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: ignored.Operation,
			Stage: ignored.Stage, Outcome: "ignored", Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
			InstallationID: adapter.InstallationID, RunnerGroupID: adapter.RunnerGroupID, ScaleSetID: adapter.ScaleSetID,
			EventType: ignored.EventType, MessageID: ignored.MessageID, SourceRequestID: ignored.SourceRequestID,
			SourceRequestIDSet: ignored.SourceRequestIDSet,
			RunnerName:         ignored.RunnerName, GitHubJobID: ignored.GitHubJobID, WorkflowRunID: ignored.WorkflowRunID,
			GitHubRunnerID: ignored.GitHubRunnerID, IdentityReason: ignored.IdentityReason})
	}
}

func (adapter *Adapter) reportMessageCount(operation, stage string, count int) {
	if count == 0 {
		return
	}
	adapter.Observer.report(ManagerActivity{Visibility: ActivityInfo, Operation: operation,
		Stage: stage, Outcome: "completed", Repository: adapter.Repository, RunnerPool: adapter.RunnerPool, Count: count})
}

func (adapter *Adapter) acquireAvailable(ctx context.Context, effects messageEffects) ([]identifiedRequest, error) {
	if len(effects.Available) == 0 {
		return nil, nil
	}
	requestIDs := make([]int64, 0, len(effects.Available))
	availableByID := make(map[int64]core.JobRequest, len(effects.Available))
	for _, request := range effects.Available {
		requestIDs = append(requestIDs, request.SourceID)
		availableByID[request.SourceID] = request.JobRequest
	}
	acquiredIDs, err := adapter.Client.AcquireRequests(ctx, requestIDs)
	if err != nil {
		return nil, fmt.Errorf("acquire GitHub available requests: %w", err)
	}
	for _, requestID := range acquiredIDs {
		if _, found := availableByID[requestID]; !found {
			return nil, fmt.Errorf("GitHub acquired unknown request %d", requestID)
		}
	}
	acquired := make([]identifiedRequest, 0, len(acquiredIDs))
	for _, requestID := range acquiredIDs {
		acquired = append(acquired, identifiedRequest{SourceID: requestID, JobRequest: availableByID[requestID]})
	}
	return acquired, nil
}

func (adapter *Adapter) persistRequests(ctx context.Context, requests []identifiedRequest) error {
	for _, request := range requests {
		if err := adapter.request(ctx, request.SourceID, request.JobRequest); err != nil {
			return err
		}
	}
	return nil
}

type managerActivityReportedError struct{ cause error }

func (reported managerActivityReportedError) Error() string { return reported.cause.Error() }
func (reported managerActivityReportedError) Unwrap() error { return reported.cause }

func markManagerActivityReported(err error) error { return managerActivityReportedError{cause: err} }

func managerActivityWasReported(err error) bool {
	var reported managerActivityReportedError
	return errors.As(err, &reported)
}

func durableStateFailureContext(err error) (backend string, operation string) {
	var stateFailure interface {
		StateBackend() string
		StateOperation() string
	}
	if !errors.As(err, &stateFailure) {
		return "", ""
	}
	return stateFailure.StateBackend(), stateFailure.StateOperation()
}

type identifiedRequest struct {
	SourceID   int64
	JobRequest core.JobRequest
}

type messageEffects struct {
	Available []identifiedRequest
	Assigned  []identifiedRequest
	Started   []identifiedObservation
	Completed []identifiedObservation
	Ignored   []ignoredEvidence
}

type ignoredEvidence struct {
	Operation          string
	Stage              string
	EventType          string
	MessageID          int64
	SourceRequestID    int64
	SourceRequestIDSet bool
	RunnerName         core.RunnerName
	GitHubJobID        string
	WorkflowRunID      int64
	GitHubRunnerID     int64
	IdentityReason     string
}

func (adapter *Adapter) validateMessage(message *upstream.RunnerScaleSetMessage) (messageEffects, error) {
	available, ignoredAvailable, err := adapter.classifyAvailable(int64(message.MessageID), message.JobAvailableMessages)
	if err != nil {
		return messageEffects{}, err
	}
	assigned, ignoredAssigned, err := adapter.classifyAssigned(int64(message.MessageID), message.JobAssignedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	started, ignoredStarted, err := adapter.validateStarted(int64(message.MessageID), message.JobStartedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	completed, ignoredCompleted, err := adapter.validateCompleted(int64(message.MessageID), message.JobCompletedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	return messageEffects{
		Available: available, Assigned: assigned, Started: started, Completed: completed,
		Ignored: append(append(append(ignoredAvailable, ignoredAssigned...), ignoredStarted...), ignoredCompleted...),
	}, nil
}

func (adapter *Adapter) validateAvailable(
	events []*upstream.JobAvailable,
) ([]identifiedRequest, error) {
	available, _, err := adapter.classifyAvailable(0, events)
	return available, err
}

func (adapter *Adapter) classifyAvailable(
	messageID int64,
	events []*upstream.JobAvailable,
) ([]identifiedRequest, []ignoredEvidence, error) {
	return classifyRequestEvents(adapter, messageID, "JobAvailable", "validate_available", events,
		firstRequestOccurrence,
		func(event *upstream.JobAvailable) *upstream.JobMessageBase { return &event.JobMessageBase })
}

func (adapter *Adapter) classifyAssigned(
	messageID int64,
	events []*upstream.JobAssigned,
) ([]identifiedRequest, []ignoredEvidence, error) {
	return classifyRequestEvents(adapter, messageID, "JobAssigned", "validate_assigned", events,
		everyRequestOccurrence,
		func(event *upstream.JobAssigned) *upstream.JobMessageBase { return &event.JobMessageBase })
}

type requestSelector func(map[int64]struct{}, int64) bool

func firstRequestOccurrence(seen map[int64]struct{}, requestID int64) bool {
	_, duplicate := seen[requestID]
	return !duplicate
}

func everyRequestOccurrence(map[int64]struct{}, int64) bool { return true }

func classifyRequestEvents[eventType any](
	adapter *Adapter,
	messageID int64,
	githubEventType string,
	stage string,
	events []*eventType,
	selectRequest requestSelector,
	baseFor func(*eventType) *upstream.JobMessageBase,
) ([]identifiedRequest, []ignoredEvidence, error) {
	actionable := make([]identifiedRequest, 0, len(events))
	ignored := make([]ignoredEvidence, 0)
	seen := make(map[int64]struct{}, len(events))
	for _, event := range events {
		classified, err := classifyRequestEvent(adapter,
			messageID, githubEventType, stage, event, seen, selectRequest, baseFor,
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
		actionable = append(actionable, classified.Request)
	}
	return actionable, ignored, nil
}

type classifiedRequestEvent struct {
	Request    identifiedRequest
	Evidence   *ignoredEvidence
	Actionable bool
}

func classifyRequestEvent[eventType any](
	adapter *Adapter,
	messageID int64,
	githubEventType string,
	stage string,
	event *eventType,
	seen map[int64]struct{},
	selectRequest requestSelector,
	baseFor func(*eventType) *upstream.JobMessageBase,
) (classifiedRequestEvent, error) {
	if event == nil {
		evidence := requestEvidence(messageID, githubEventType, stage, nil, "nil_event")
		return classifiedRequestEvent{Evidence: &evidence}, nil
	}
	base := baseFor(event)
	if base.RunnerRequestID <= 0 {
		evidence := requestEvidence(messageID, githubEventType, stage, base, "non_positive_request_id")
		return classifiedRequestEvent{Evidence: &evidence}, nil
	}
	if !selectRequest(seen, base.RunnerRequestID) {
		return classifiedRequestEvent{}, nil
	}
	request, err := adapter.jobRequest(base.RunnerRequestID, base.QueueTime)
	if err != nil {
		return classifiedRequestEvent{}, err
	}
	seen[base.RunnerRequestID] = struct{}{}
	return classifiedRequestEvent{Request: identifiedRequest{SourceID: base.RunnerRequestID, JobRequest: request},
		Actionable: true}, nil
}

func requestEvidence(
	messageID int64,
	eventType string,
	stage string,
	base *upstream.JobMessageBase,
	reason string,
) ignoredEvidence {
	evidence := ignoredEvidence{Operation: "job_ingestion", Stage: stage, EventType: eventType,
		MessageID: messageID, IdentityReason: reason}
	if base != nil {
		evidence.SourceRequestID = base.RunnerRequestID
		evidence.SourceRequestIDSet = true
		evidence.GitHubJobID = base.JobID
		evidence.WorkflowRunID = base.WorkflowRunID
	}
	return evidence
}

func (adapter *Adapter) jobRequest(requestID int64, enqueuedAt time.Time) (core.JobRequest, error) {
	source, err := adapter.sourceReference(requestID)
	if err != nil {
		return core.JobRequest{}, err
	}
	return core.JobRequest{Source: source, RunnerPool: adapter.RunnerPool, EnqueuedAt: enqueuedAt}, nil
}

func (adapter *Adapter) request(ctx context.Context, requestID int64, request core.JobRequest) error {
	adapter.Observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "job_ingestion",
		Stage: "persist_job", Outcome: "started", Repository: adapter.Repository,
		RunnerPool: adapter.RunnerPool, SourceRequestID: requestID, SourceRequestIDSet: true})
	result, err := adapter.Handler.Request(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("durably handle GitHub request %d: %w", requestID, err)
		}
		activity := ManagerActivity{Visibility: ActivityInfo, Operation: "job_ingestion",
			Stage: "persist_job", Outcome: "failed", Repository: adapter.Repository,
			RunnerPool: adapter.RunnerPool, SourceRequestID: requestID, SourceRequestIDSet: true, Err: err}
		activity.StateBackend, activity.StateOperation = durableStateFailureContext(err)
		adapter.Observer.report(activity)
		return markManagerActivityReported(fmt.Errorf("durably handle GitHub request %d: %w", requestID, err))
	}
	visibility := ActivityDebug
	if result.Outcome == core.QueueQueued {
		visibility = ActivityInfo
	}
	adapter.Observer.report(ManagerActivity{Visibility: visibility, Operation: "job_ingestion",
		Stage: "persist_job", Outcome: string(result.Outcome), Repository: adapter.Repository,
		RunnerPool: adapter.RunnerPool, SourceRequestID: requestID, SourceRequestIDSet: true, JobID: result.Job.ID})
	return nil
}

func (adapter *Adapter) sourceReference(requestID int64) (core.SourceReference, error) {
	sourceID, err := EncodeSourceID(SourceIdentity{
		Repository: adapter.Repository,
		RunnerPool: adapter.RunnerPool,
		RequestID:  requestID,
	})
	if err != nil {
		return core.SourceReference{}, fmt.Errorf("identify GitHub request %s: %w", strconv.FormatInt(requestID, 10), err)
	}
	return core.SourceReference{Adapter: AdapterName, ID: sourceID}, nil
}
