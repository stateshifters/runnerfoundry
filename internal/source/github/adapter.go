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
	Repository    string
	RunnerPool    core.RunnerPool
	LastMessageID int
}

// PollOnce receives and processes at most one GitHub message. It acknowledges
// the message only after every translated event has been durably handled. The
// returned boolean reports whether GitHub supplied a message, including when
// durable handling or acknowledgement subsequently fails.
func (adapter *Adapter) PollOnce(ctx context.Context, maxCapacity int) (bool, error) {
	if adapter.Client == nil {
		return false, errors.New("GitHub message client is required")
	}
	if adapter.Handler == nil {
		return false, errors.New("GitHub event handler is required")
	}
	message, err := adapter.Client.GetMessage(ctx, adapter.LastMessageID, maxCapacity)
	if errors.Is(err, ErrNoMessage) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("receive GitHub scale-set message: %w", err)
	}
	if message == nil {
		return false, errors.New("GitHub message client returned nil without ErrNoMessage")
	}
	if err := adapter.handleMessage(ctx, message); err != nil {
		return true, err
	}
	if err := adapter.Client.DeleteMessage(ctx, message.MessageID); err != nil {
		return true, fmt.Errorf("acknowledge GitHub scale-set message %d: %w", message.MessageID, err)
	}
	adapter.LastMessageID = message.MessageID
	return true, nil
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
	if err := adapter.persistStarted(ctx, effects.Started); err != nil {
		return err
	}
	return adapter.persistCompleted(ctx, effects.Completed)
}

func (adapter *Adapter) acquireAvailable(ctx context.Context, effects messageEffects) ([]identifiedRequest, error) {
	if len(effects.AvailableIDs) == 0 {
		return nil, nil
	}
	acquiredIDs, err := adapter.Client.AcquireRequests(ctx, effects.AvailableIDs)
	if err != nil {
		return nil, fmt.Errorf("acquire GitHub available requests: %w", err)
	}
	for _, requestID := range acquiredIDs {
		if _, found := effects.Available[requestID]; !found {
			return nil, fmt.Errorf("GitHub acquired unknown request %d", requestID)
		}
	}
	acquired := make([]identifiedRequest, 0, len(acquiredIDs))
	for _, requestID := range acquiredIDs {
		acquired = append(acquired, identifiedRequest{SourceID: requestID, JobRequest: effects.Available[requestID]})
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
	Available    map[int64]core.JobRequest
	AvailableIDs []int64
	Assigned     []identifiedRequest
	Started      []identifiedObservation
	Completed    []identifiedObservation
}

func (adapter *Adapter) validateMessage(message *upstream.RunnerScaleSetMessage) (messageEffects, error) {
	available, requestIDs, err := adapter.validateAvailable(message.JobAvailableMessages)
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
		Available: available, AvailableIDs: requestIDs, Assigned: assigned, Started: started, Completed: completed,
	}, nil
}

func (adapter *Adapter) validateAvailable(
	events []*upstream.JobAvailable,
) (map[int64]core.JobRequest, []int64, error) {
	available := make(map[int64]core.JobRequest, len(events))
	requestIDs := make([]int64, 0, len(events))
	for _, event := range events {
		if event == nil {
			return nil, nil, errors.New("GitHub JobAvailable message is nil")
		}
		request, err := adapter.jobRequest(event.RunnerRequestID, event.QueueTime)
		if err != nil {
			return nil, nil, err
		}
		if _, duplicate := available[event.RunnerRequestID]; !duplicate {
			requestIDs = append(requestIDs, event.RunnerRequestID)
		}
		available[event.RunnerRequestID] = request
	}
	return available, requestIDs, nil
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
	if err := adapter.Handler.Request(ctx, request); err != nil {
		return fmt.Errorf("durably handle GitHub request %d: %w", requestID, err)
	}
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
