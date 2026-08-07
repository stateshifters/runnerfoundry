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
	Request(ctx context.Context, request core.JobRequest) error
	RunnerStarted(ctx context.Context, source core.SourceReference) error
	Terminalize(ctx context.Context, source core.SourceReference) error
}

// Adapter receives GitHub scale-set messages and translates their job events
// into source-neutral core requests and observations. Statistics and assigned
// counts are capacity observations only and never create job demand.
//
// PollOnce is not safe for concurrent calls on the same Adapter because the
// acknowledgement cursor is mutable. A failed handler or acknowledgement
// leaves LastMessageID unchanged so GitHub can redeliver the message.
type Adapter struct {
	Client        MessageClient
	Handler       EventHandler
	Observer      ManagerObserver
	Repository    string
	RunnerPool    core.RunnerPool
	LastMessageID int
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
	adapter.reportPoll("poll", "started", pollStarted, nil)
	if err := adapter.validatePollBoundaries(); err != nil {
		return PollResult{}, err
	}
	message, empty, err := adapter.receiveMessage(ctx, maxCapacity, pollStarted)
	if err != nil {
		return PollResult{}, err
	}
	if empty {
		return PollResult{}, nil
	}
	result := PollResult{Received: true}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("GitHub poll canceled before message handling: %w", err)
	}
	if err := adapter.handleMessage(ctx, message); err != nil {
		return result, err
	}
	result.Handled = true
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("GitHub poll canceled before message acknowledgement: %w", err)
	}
	if err := adapter.acknowledgeMessage(ctx, message.MessageID, pollStarted); err != nil {
		return result, err
	}
	adapter.LastMessageID = message.MessageID
	result.Acknowledged = true
	adapter.reportPoll("poll", "completed", pollStarted, nil)
	return result, nil
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
		adapter.reportPoll("poll", "empty", started, nil)
		return nil, true, nil
	}
	if err != nil {
		adapter.reportPoll("poll", "failed", started, err)
		return nil, false, fmt.Errorf("receive GitHub scale-set message: %w", err)
	}
	if message == nil {
		return nil, false, errors.New("GitHub message client returned nil without ErrNoMessage")
	}
	return message, false, nil
}

func (adapter *Adapter) acknowledgeMessage(ctx context.Context, messageID int, started time.Time) error {
	if err := adapter.Client.DeleteMessage(ctx, messageID); err != nil {
		adapter.reportPoll("acknowledge_message", "failed", started, err)
		return fmt.Errorf("acknowledge GitHub scale-set message %d: %w", messageID, err)
	}
	adapter.reportPoll("acknowledge_message", "completed", started, nil)
	return nil
}

func (adapter *Adapter) reportPoll(stage, outcome string, started time.Time, err error) {
	activity := ManagerActivity{Visibility: ActivityDebug, Operation: "source_poll", Stage: stage, Outcome: outcome,
		Repository: adapter.Repository, RunnerPool: adapter.RunnerPool, Err: err}
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

func (adapter *Adapter) persistStarted(ctx context.Context, started []identifiedObservation) error {
	for _, observation := range started {
		if err := adapter.Handler.RunnerStarted(ctx, observation.Source); err != nil {
			return fmt.Errorf("durably record GitHub request %d started: %w", observation.RequestID, err)
		}
	}
	return nil
}

func (adapter *Adapter) persistCompleted(ctx context.Context, completed []identifiedObservation) error {
	for _, observation := range completed {
		if err := adapter.Handler.Terminalize(ctx, observation.Source); err != nil {
			return fmt.Errorf("durably terminalize GitHub request %d: %w", observation.RequestID, err)
		}
	}
	return nil
}

type identifiedRequest struct {
	SourceID   int64
	JobRequest core.JobRequest
}

type identifiedObservation struct {
	RequestID int64
	Source    core.SourceReference
}

type messageEffects struct {
	Available []identifiedRequest
	Assigned  []identifiedRequest
	Started   []identifiedObservation
	Completed []identifiedObservation
}

func (adapter *Adapter) validateMessage(message *upstream.RunnerScaleSetMessage) (messageEffects, error) {
	available, err := adapter.validateAvailable(message.JobAvailableMessages)
	if err != nil {
		return messageEffects{}, err
	}
	assigned, err := adapter.validateAssigned(message.JobAssignedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	started, err := adapter.validateStarted(message.JobStartedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	completed, err := adapter.validateCompleted(message.JobCompletedMessages)
	if err != nil {
		return messageEffects{}, err
	}
	return messageEffects{
		Available: available, Assigned: assigned, Started: started, Completed: completed,
	}, nil
}

func (adapter *Adapter) validateAvailable(
	events []*upstream.JobAvailable,
) ([]identifiedRequest, error) {
	available := make([]identifiedRequest, 0, len(events))
	seen := make(map[int64]struct{}, len(events))
	for _, event := range events {
		if event == nil {
			return nil, errors.New("GitHub JobAvailable message is nil")
		}
		request, err := adapter.jobRequest(event.RunnerRequestID, event.QueueTime)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[event.RunnerRequestID]; duplicate {
			continue
		}
		seen[event.RunnerRequestID] = struct{}{}
		available = append(available, identifiedRequest{SourceID: event.RunnerRequestID, JobRequest: request})
	}
	return available, nil
}

func (adapter *Adapter) validateAssigned(events []*upstream.JobAssigned) ([]identifiedRequest, error) {
	assigned := make([]identifiedRequest, 0, len(events))
	for _, event := range events {
		if event == nil {
			return nil, errors.New("GitHub JobAssigned message is nil")
		}
		request, err := adapter.jobRequest(event.RunnerRequestID, event.QueueTime)
		if err != nil {
			return nil, err
		}
		assigned = append(assigned, identifiedRequest{SourceID: event.RunnerRequestID, JobRequest: request})
	}
	return assigned, nil
}

func (adapter *Adapter) validateStarted(events []*upstream.JobStarted) ([]identifiedObservation, error) {
	started := make([]identifiedObservation, 0, len(events))
	for _, event := range events {
		if event == nil {
			return nil, errors.New("GitHub JobStarted message is nil")
		}
		source, err := adapter.sourceReference(event.RunnerRequestID)
		if err != nil {
			return nil, err
		}
		started = append(started, identifiedObservation{RequestID: event.RunnerRequestID, Source: source})
	}
	return started, nil
}

func (adapter *Adapter) validateCompleted(
	events []*upstream.JobCompleted,
) ([]identifiedObservation, error) {
	completed := make([]identifiedObservation, 0, len(events))
	for _, event := range events {
		if event == nil {
			return nil, errors.New("GitHub JobCompleted message is nil")
		}
		source, err := adapter.sourceReference(event.RunnerRequestID)
		if err != nil {
			return nil, err
		}
		completed = append(completed, identifiedObservation{RequestID: event.RunnerRequestID, Source: source})
	}
	return completed, nil
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
		RunnerPool: adapter.RunnerPool, SourceRequestID: requestID})
	if err := adapter.Handler.Request(ctx, request); err != nil {
		adapter.Observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "job_ingestion",
			Stage: "persist_job", Outcome: "failed", Repository: adapter.Repository,
			RunnerPool: adapter.RunnerPool, SourceRequestID: requestID, Err: err})
		return fmt.Errorf("durably handle GitHub request %d: %w", requestID, err)
	}
	adapter.Observer.report(ManagerActivity{Visibility: ActivityDebug, Operation: "job_ingestion",
		Stage: "persist_job", Outcome: "completed", Repository: adapter.Repository,
		RunnerPool: adapter.RunnerPool, SourceRequestID: requestID})
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
