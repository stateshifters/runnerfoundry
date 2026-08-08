package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	upstream "github.com/actions/scaleset"
)

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
	result := PollResult{}
	adapter.reportPoll("poll", "started", 0, pollStarted, nil)
	if err := adapter.validatePollBoundaries(); err != nil {
		adapter.reportPoll("poll_configuration", "failed", 0, pollStarted, err)
		return result, err
	}
	demandChanged, err := adapter.reconcileInitialAssignedDemand(ctx, pollStarted)
	if err != nil {
		return result, err
	}
	result.DemandChanged = demandChanged
	if demandChanged {
		adapter.reportPoll("poll", "completed", 0, pollStarted, nil)
		return result, nil
	}
	message, empty, err := adapter.receiveMessage(ctx, maxCapacity, pollStarted)
	if err != nil {
		return result, err
	}
	if empty {
		return result, nil
	}
	messageResult, err := adapter.handleReceivedMessage(ctx, message, pollStarted)
	messageResult.DemandChanged = messageResult.DemandChanged || result.DemandChanged
	return messageResult, err
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
	demandChanged, err := adapter.handleMessage(ctx, message, pollStarted)
	if err != nil {
		return result, adapter.classifyMessageHandlingFailure(ctx, message.MessageID, pollStarted, err)
	}
	result.DemandChanged = demandChanged
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

func (adapter *Adapter) requireActivePoll(ctx context.Context, stage string, messageID int, pollStarted time.Time) error {
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

func (adapter *Adapter) receiveMessage(
	ctx context.Context,
	maxCapacity int,
	started time.Time,
) (*upstream.RunnerScaleSetMessage, bool, error) {
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
