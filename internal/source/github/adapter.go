// Package github translates GitHub Actions runner scale-set messages at the
// source adapter boundary.
package github

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
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

// AssignedDemandHandler durably reconciles GitHub's aggregate assigned-job
// observation after actionable lifecycle events have been applied.
type AssignedDemandHandler interface {
	core.ConditionalTerminalObserver
	ReconcileAssignedDemand(ctx context.Context, demand AssignedDemand) (AssignedDemandResult, error)
}

func isValidAssignedDemandHandler(handler AssignedDemandHandler) bool {
	if handler == nil {
		return false
	}
	value := reflect.ValueOf(handler)
	kind := value.Kind()
	nilable := kind == reflect.Chan || kind == reflect.Func || kind == reflect.Interface ||
		kind == reflect.Map || kind == reflect.Pointer || kind == reflect.Slice
	return !nilable || !value.IsNil()
}

// Adapter translates exact GitHub lifecycle events and authoritative
// assigned-job statistics into durable demand and source-neutral observations.
//
// PollOnce is not safe for concurrent calls on the same Adapter because the
// acknowledgement cursor is mutable. A failed handler or acknowledgement
// leaves LastMessageID unchanged so GitHub can redeliver the message.
type Adapter struct {
	Client                   MessageClient
	SourceEvents             core.SourceEventHandler
	AssignedDemand           AssignedDemandHandler
	Observer                 ManagerObserver
	Repository               string
	RunnerPool               core.RunnerPool
	InstallationID           int64
	RunnerGroupID            int64
	ScaleSetID               int64
	LastMessageID            int
	InitialSessionID         uuid.UUID
	InitialStatistics        *upstream.RunnerScaleSetStatistic
	InitialStatisticsPending bool
}

// PollResult distinguishes receipt, durable handling, and acknowledgement.
// A false Acknowledged value means the message remains eligible for redelivery.
type PollResult struct {
	Received      bool
	Handled       bool
	Acknowledged  bool
	DemandChanged bool
}

func (adapter *Adapter) handleMessage(
	ctx context.Context,
	message *upstream.RunnerScaleSetMessage,
	observedAt time.Time,
) (bool, error) {
	if message.Statistics == nil {
		return false, fmt.Errorf("GitHub scale-set message %d is missing statistics", message.MessageID)
	}
	effects, err := adapter.validateMessage(message)
	if err != nil {
		return false, err
	}
	if err := adapter.applyMessageEffects(ctx, effects); err != nil {
		return false, err
	}
	return adapter.reconcileAssignedDemand(ctx, message.Statistics,
		"message:"+strconv.Itoa(message.MessageID), int64(message.MessageID), observedAt)
}

func (adapter *Adapter) applyMessageEffects(ctx context.Context, effects messageEffects) error {
	adapter.reportIgnoredEvidence(effects.Ignored)
	acquired, err := adapter.acquireAvailable(ctx, effects)
	if err != nil {
		return err
	}
	if err := adapter.persistRequests(ctx, acquired); err != nil {
		return err
	}
	adapter.reportMessageCount("job_ingestion", "enqueue_jobs", len(acquired))
	if err := adapter.persistAccepted(ctx, effects.Started); err != nil {
		return err
	}
	if err := adapter.persistCompleted(ctx, effects.Completed); err != nil {
		return err
	}
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
	started, ignoredStarted, err := adapter.validateStarted(int64(message.MessageID), message.JobStartedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	completed, ignoredCompleted, err := adapter.validateCompleted(int64(message.MessageID), message.JobCompletedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	return messageEffects{
		Available: available, Started: started, Completed: completed,
		Ignored: append(append(ignoredAvailable, ignoredStarted...), ignoredCompleted...),
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
		func(event *upstream.JobAvailable) *upstream.JobMessageBase { return &event.JobMessageBase })
}

func classifyRequestEvents[eventType any](
	adapter *Adapter,
	messageID int64,
	githubEventType string,
	stage string,
	events []*eventType,
	baseFor func(*eventType) *upstream.JobMessageBase,
) ([]identifiedRequest, []ignoredEvidence, error) {
	actionable := make([]identifiedRequest, 0, len(events))
	ignored := make([]ignoredEvidence, 0)
	seen := make(map[int64]struct{}, len(events))
	for _, event := range events {
		classified, err := classifyRequestEvent(adapter,
			messageID, githubEventType, stage, event, seen, baseFor,
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
	if _, duplicate := seen[base.RunnerRequestID]; duplicate {
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
	identity := SourceIdentity{Repository: adapter.Repository, RunnerPool: adapter.RunnerPool, RequestID: requestID}
	sourceID, err := EncodeSourceID(identity)
	if err != nil {
		return core.JobRequest{}, err
	}
	return core.JobRequest{Source: core.SourceReference{Adapter: AdapterName, ID: sourceID}, Scope: scopeForIdentity(identity),
		RunnerPool: adapter.RunnerPool, EnqueuedAt: enqueuedAt}, nil
}

func (adapter *Adapter) request(ctx context.Context, requestID int64, request core.JobRequest) error {
	adapter.Observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "job_ingestion",
		Stage: "persist_job", Outcome: "started", Repository: adapter.Repository,
		RunnerPool: adapter.RunnerPool, SourceRequestID: requestID, SourceRequestIDSet: true})
	result, err := adapter.ingestDemand(ctx, request)
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

func (adapter *Adapter) ingestDemand(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	result, err := adapter.SourceEvents.IngestDemand(ctx, request)
	if err != nil {
		return result, fmt.Errorf("ingest source-neutral demand: %w", err)
	}
	return result, nil
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
