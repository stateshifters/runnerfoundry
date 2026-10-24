package orchard

import (
	"context"
	"errors"
	"fmt"
	"time"

	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
)

type GuestConnector interface {
	Connect(ctx context.Context, name string) (Guest, error)
}
type Guest struct {
	Exec  func(ctx context.Context) (ExecStream, error)
	Close func() error
}
type ExecStream struct {
	Send      func(request *guestagent.ExecRequest) error
	CloseSend func() error
	Recv      func() (*guestagent.ExecResponse, error)
}

func (provider *Provider) deliver(ctx context.Context, name string, content []byte) (deliveryError error) {
	guest, err := provider.guest.Connect(ctx, name)
	if err != nil {
		return errors.New("connect to Orchard guest failed")
	}
	defer func() {
		if err := guest.Close(); err != nil {
			deliveryError = errors.Join(deliveryError, errors.New("close Orchard guest failed"))
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for Orchard guest: %w", err)
		}
		err := execute(ctx, guest, &guestagent.ExecRequest_Command{Name: "/bin/test", Args: []string{"-p", "/var/run/runnerfoundry/bootstrap"}}, nil)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Orchard guest: %w", ctx.Err())
		case <-provider.after(100 * time.Millisecond):
		}
	}
	return execute(ctx, guest, &guestagent.ExecRequest_Command{Name: "/bin/sh", Args: []string{"-c", "test -p /var/run/runnerfoundry/bootstrap && exec /bin/cat > /var/run/runnerfoundry/bootstrap"}, Interactive: true}, content)
}

func execute(ctx context.Context, guest Guest, command *guestagent.ExecRequest_Command, content []byte) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := guest.Exec(ctx)
	if err != nil {
		return errors.New("open guest execution failed")
	}
	if err := stream.Send(&guestagent.ExecRequest{Type: &guestagent.ExecRequest_Command_{Command: command}}); err != nil {
		return errors.New("send guest command failed")
	}
	if err := sendGuestInput(stream, content); err != nil {
		return err
	}
	if err := receiveGuestExit(stream); err != nil {
		return err
	}
	return guestContextError(ctx)
}

func sendGuestInput(stream ExecStream, content []byte) error {
	for offset := 0; offset < len(content); {
		end := min(offset+32*1024, len(content))
		if err := stream.Send(&guestagent.ExecRequest{Type: &guestagent.ExecRequest_StandardInput{StandardInput: &guestagent.IOChunk{Data: content[offset:end]}}}); err != nil {
			return errors.New("send guest input failed")
		}
		offset = end
	}
	if err := stream.CloseSend(); err != nil {
		return errors.New("close guest input failed")
	}
	return nil
}

func receiveGuestExit(stream ExecStream) error {
	for {
		response, err := stream.Recv()
		if err != nil {
			return errors.New("receive guest result failed")
		}
		if response == nil {
			return errors.New("missing guest result")
		}
		result, exited := response.GetType().(*guestagent.ExecResponse_Exit_)
		if !exited {
			continue
		}
		if result.Exit == nil || result.Exit.Code != 0 {
			return errors.New("guest command failed")
		}
		return nil
	}
}

func guestContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("guest execution canceled: %w", err)
	}
	return nil
}
