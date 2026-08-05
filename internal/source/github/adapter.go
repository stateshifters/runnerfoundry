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
	DeleteMessage(ctx context.Context, messageID int) error
}

// EventHandler durably accepts source-neutral events translated from GitHub.
// A method must return nil only after its event can survive process failure;
// Adapter acknowledges the containing GitHub message only after every method
// invoked for that message succeeds. Implementations must handle redelivery
// idempotently because acknowledgement can fail after durable handling.
type EventHandler interface {
	Request(ctx context.Context, request core.JobRequest) error
	Started(ctx context.Context, source core.SourceReference, observedAt time.Time) error
	Completed(ctx context.Context, source core.SourceReference, result core.Result) error
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
	if err := adapter.handleAvailable(ctx, message.JobAvailableMessages); err != nil {
		return err
	}
	if err := adapter.handleStarted(ctx, message.JobStartedMessages); err != nil {
		return err
	}
	return adapter.handleCompleted(ctx, message.JobCompletedMessages)
}

func (adapter *Adapter) handleAvailable(ctx context.Context, messages []*upstream.JobAvailable) error {
	var handlingErrors []error
	for _, available := range messages {
		if available == nil {
			handlingErrors = append(handlingErrors, errors.New("GitHub JobAvailable message is nil"))
			continue
		}
		source, err := adapter.sourceReference(available.RunnerRequestID)
		if err != nil {
			handlingErrors = append(handlingErrors, err)
			continue
		}
		request := core.JobRequest{
			Source:     source,
			RunnerPool: adapter.RunnerPool,
			EnqueuedAt: available.QueueTime,
		}
		if err := adapter.Handler.Request(ctx, request); err != nil {
			handlingErrors = append(
				handlingErrors,
				fmt.Errorf("durably handle GitHub JobAvailable request %d: %w", available.RunnerRequestID, err),
			)
		}
	}
	return errors.Join(handlingErrors...)
}

func (adapter *Adapter) handleStarted(ctx context.Context, messages []*upstream.JobStarted) error {
	for _, started := range messages {
		if started == nil {
			return errors.New("GitHub JobStarted message is nil")
		}
		source, err := adapter.sourceReference(started.RunnerRequestID)
		if err != nil {
			return err
		}
		if err := adapter.Handler.Started(ctx, source, started.RunnerAssignTime); err != nil {
			return fmt.Errorf("durably handle GitHub JobStarted request %d: %w", started.RunnerRequestID, err)
		}
	}
	return nil
}

func (adapter *Adapter) handleCompleted(ctx context.Context, messages []*upstream.JobCompleted) error {
	for _, completed := range messages {
		if completed == nil {
			return errors.New("GitHub JobCompleted message is nil")
		}
		source, err := adapter.sourceReference(completed.RunnerRequestID)
		if err != nil {
			return err
		}
		if err := adapter.Handler.Completed(ctx, source, translateResult(completed)); err != nil {
			return fmt.Errorf("durably handle GitHub JobCompleted request %d: %w", completed.RunnerRequestID, err)
		}
	}
	return nil
}

// ReportResult classifies delivery of a terminal result to GitHub. GitHub's
// runner reports its own result, so jobs originating from this adapter require
// no second API report. Jobs from another adapter are unsupported here.
func (adapter Adapter) ReportResult(
	_ context.Context,
	job core.Job,
	result core.Result,
) (core.ResultDelivery, error) {
	return resultDelivery(job, result), nil
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

func translateResult(completed *upstream.JobCompleted) core.Result {
	if completed.Result == "succeeded" {
		return core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner}
	}
	return core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginRunner, Reason: completed.Result}
}
