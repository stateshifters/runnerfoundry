package scaleset

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
)

type MessageClient interface {
	GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error)
	DeleteMessage(ctx context.Context, messageID int) error
}

// ErrNoMessage identifies a successful poll that returned no work.
// MessageClient adapters return it instead of an ambiguous nil message.
var ErrNoMessage = errors.New("no scale set message")

type MessageHandler interface {
	HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error
}

// Poller receives one GitHub scale-set message and acknowledges it only after
// the handler succeeds.
//
// DeleteMessage is the acknowledgement boundary. A failed handler leaves the
// message unacknowledged so GitHub can redeliver demand instead of losing work.
type Poller struct {
	Client        MessageClient
	Handler       MessageHandler
	MaxCapacity   int
	LastMessageID int
}

func (poller *Poller) PollOnce(ctx context.Context) error {
	if poller.Client == nil {
		return errors.New("message client is required")
	}
	if poller.Handler == nil {
		return errors.New("message handler is required")
	}
	message, err := poller.Client.GetMessage(ctx, poller.LastMessageID, poller.MaxCapacity)
	if errors.Is(err, ErrNoMessage) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get scale set message: %w", err)
	}
	if message == nil {
		return errors.New("message client returned nil without ErrNoMessage")
	}
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	if err := poller.Handler.HandleMessage(ctx, message); err != nil {
		return fmt.Errorf("handle scale set message: %w", err)
	}
	if err := poller.Client.DeleteMessage(ctx, message.MessageID); err != nil {
		return fmt.Errorf("acknowledge scale set message: %w", err)
	}
	poller.LastMessageID = message.MessageID
	return nil
}
