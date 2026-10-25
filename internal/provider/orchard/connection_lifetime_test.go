package orchard

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestGuestForwardSurvivesCompletedGRPCDialUntilGuestClose(t *testing.T) {
	parent := context.Background()
	var forwardedContext context.Context
	connection := &lifetimeConnection{}
	service := &fakeVMService{onForward: func(ctx context.Context) (net.Conn, error) { forwardedContext = ctx; return connection, nil }}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		dialContext, completeDial := context.WithCancel(context.Background())
		forwarded, err := dial(dialContext, "guest")
		completeDial()
		if err != nil {
			return Guest{}, err
		}
		return Guest{Close: forwarded.Close}, nil
	}}
	guest, err := connector.Connect(parent, "runner")
	if err != nil {
		t.Fatal(err)
	}
	if forwardedContext.Err() != nil {
		t.Fatal("completed gRPC dial canceled established guest connection")
	}
	if closeError := guest.Close(); closeError != nil {
		t.Fatal(closeError)
	}
	if forwardedContext.Err() == nil || !connection.closed {
		t.Fatal("guest close did not release connection and context")
	}
}

func TestGuestForwardRemainsBoundToStartCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var forwardedContext context.Context
	service := &fakeVMService{onForward: func(ctx context.Context) (net.Conn, error) { forwardedContext = ctx; return &lifetimeConnection{}, nil }}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		connection, err := dial(context.Background(), "guest")
		if err != nil {
			return Guest{}, err
		}
		return Guest{Close: connection.Close}, nil
	}}
	guest, err := connector.Connect(parent, "runner")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if !errors.Is(forwardedContext.Err(), context.Canceled) {
		t.Fatal("Start cancellation did not cancel guest connection")
	}
	if closeError := guest.Close(); closeError != nil {
		t.Fatal(closeError)
	}
}

func TestDialAttemptCancellationStopsPendingGuestHandshake(t *testing.T) {
	dialContext, cancelDial := context.WithCancel(context.Background())
	defer cancelDial()
	service := &fakeVMService{onForward: func(ctx context.Context) (net.Conn, error) { cancelDial(); <-ctx.Done(); return nil, ctx.Err() }}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		_, err := dial(dialContext, "guest")
		return Guest{}, err
	}}
	if _, err := connector.Connect(context.Background(), "runner"); err == nil {
		t.Fatal("canceled handshake accepted")
	}
}

func TestCanceledDialCannotPublishRacingSuccessfulForward(t *testing.T) {
	dialContext, cancelDial := context.WithCancel(context.Background())
	defer cancelDial()
	connection := &lifetimeConnection{closeError: errors.New("sensitive close failure")}
	service := &fakeVMService{onForward: func(context.Context) (net.Conn, error) { cancelDial(); return connection, nil }}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		_, err := dial(dialContext, "guest")
		return Guest{}, err
	}}
	if _, err := connector.Connect(context.Background(), "runner"); err == nil || !connection.closed {
		t.Fatal("canceled handshake leaked or published its connection")
	}
}

func TestFailedGuestHandshakeReleasesItsContext(t *testing.T) {
	var forwardedContext context.Context
	service := &fakeVMService{onForward: func(ctx context.Context) (net.Conn, error) {
		forwardedContext = ctx
		return nil, errors.New("handshake failed")
	}}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		_, err := dial(context.Background(), "guest")
		return Guest{}, err
	}}
	if _, err := connector.Connect(context.Background(), "runner"); err == nil || forwardedContext.Err() == nil {
		t.Fatal("failed handshake retained its context")
	}
}

func TestClosingOneForwardDoesNotCancelReplacementConnections(t *testing.T) {
	var forwardedContexts []context.Context
	service := &fakeVMService{onForward: func(ctx context.Context) (net.Conn, error) {
		forwardedContexts = append(forwardedContexts, ctx)
		return &lifetimeConnection{}, nil
	}}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		first, err := dial(context.Background(), "guest")
		if err != nil {
			return Guest{}, err
		}
		if closeError := first.Close(); closeError != nil {
			return Guest{}, sdkError(closeError)
		}
		second, err := dial(context.Background(), "guest")
		if err != nil {
			return Guest{}, err
		}
		return Guest{Close: second.Close}, nil
	}}
	guest, err := connector.Connect(context.Background(), "runner")
	if err != nil {
		t.Fatal(err)
	}
	if forwardedContexts[0].Err() == nil || forwardedContexts[1].Err() != nil {
		t.Fatal("forward contexts do not have independent connection ownership")
	}
	if closeError := guest.Close(); closeError != nil {
		t.Fatal(closeError)
	}
}

type deadlineContext struct {
	deadline time.Time
}

func (ctx deadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }
func (ctx deadlineContext) Done() <-chan struct{}       { return nil }
func (ctx deadlineContext) Err() error                  { return nil }
func (ctx deadlineContext) Value(_ any) any             { return nil }

type lifetimeConnection struct {
	net.Conn
	closed     bool
	closeError error
}

func (connection *lifetimeConnection) Close() error {
	connection.closed = true
	return connection.closeError
}

func TestGuestForwardPreservesStartDeadline(t *testing.T) {
	parent := deadlineContext{deadline: time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)}
	service := &fakeVMService{onForward: func(ctx context.Context) (net.Conn, error) {
		if deadline, present := ctx.Deadline(); !present || !deadline.Equal(parent.deadline) {
			t.Fatal("Start deadline lost from guest connection")
		}
		return &lifetimeConnection{}, nil
	}}
	connector := SDKConnector{vms: service, newConnection: func(dial func(context.Context, string) (net.Conn, error)) (Guest, error) {
		connection, err := dial(context.Background(), "guest")
		if err != nil {
			return Guest{}, err
		}
		return Guest{Close: connection.Close}, nil
	}}
	guest, err := connector.Connect(parent, "runner")
	if err != nil {
		t.Fatal(err)
	}
	if closeError := guest.Close(); closeError != nil {
		t.Fatal(closeError)
	}
}
