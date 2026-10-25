package orchard

import (
	"context"
	"io"
	"testing"

	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"google.golang.org/protobuf/proto"
)

func TestStartDeliversBootstrapOnlyThroughGuestStandardInput(t *testing.T) {
	ready := &fakeStream{responses: []*guestagent.ExecResponse{exitResponse(0)}}
	delivery := &fakeStream{responses: []*guestagent.ExecResponse{exitResponse(0)}}
	guest := &fakeGuest{streams: []*fakeStream{ready, delivery}}
	api := &fakeAPI{}
	provider, err := NewProvider(api, &fakeConnector{guest: guest}, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(`{"image":"base","labels":{"region":"west"}}`)})
	if err != nil {
		t.Fatal(err)
	}
	err = provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName, Bootstrap: core.NewBootstrap([]byte("secret-bootstrap"))})
	if err != nil {
		t.Fatal(err)
	}
	if len(api.created) != 1 || api.created[0].Name != "runnerfoundry-orchard-v1-cG9vbA-"+string(runnerName) || api.created[0].Labels["region"] != "west" {
		t.Fatalf("create=%+v", api.created)
	}
	assertGuestFIFOReadiness(t, ready)
	assertGuestBootstrapDelivery(t, delivery)
	if !guest.closed {
		t.Fatal("guest connection not closed")
	}
}

func assertGuestFIFOReadiness(t *testing.T, stream *fakeStream) {
	t.Helper()
	expected := &guestagent.ExecRequest_Command{Name: "/bin/test", Args: []string{"-p", "/var/run/runnerfoundry/bootstrap"}}
	if len(stream.requests) != 1 || !proto.Equal(stream.requests[0].GetCommand(), expected) {
		t.Fatal("missing FIFO readiness")
	}
}

func assertGuestBootstrapDelivery(t *testing.T, stream *fakeStream) {
	t.Helper()
	if len(stream.requests) != 2 || string(stream.requests[1].GetStandardInput().GetData()) != "secret-bootstrap" || !stream.closed {
		t.Fatalf("delivery=%+v", stream)
	}
	expected := &guestagent.ExecRequest_Command{Name: "/bin/sh", Args: []string{"-c", "test -p /var/run/runnerfoundry/bootstrap && exec /bin/cat > /var/run/runnerfoundry/bootstrap"}, Interactive: true}
	if !proto.Equal(stream.requests[0].GetCommand(), expected) {
		t.Fatalf("command=%+v", stream.requests[0].GetCommand())
	}
}

type fakeConnector struct {
	guest *fakeGuest
	err   error
}

func (connector *fakeConnector) Connect(context.Context, string) (Guest, error) {
	if connector.err != nil {
		return Guest{}, connector.err
	}
	return Guest{Exec: connector.guest.Exec, Close: connector.guest.Close}, nil
}

type fakeGuest struct {
	streams    []*fakeStream
	err        error
	closed     bool
	closeError error
}

func (guest *fakeGuest) Exec(context.Context) (ExecStream, error) {
	if guest.err != nil {
		return ExecStream{}, guest.err
	}
	stream := guest.streams[0]
	guest.streams = guest.streams[1:]
	return ExecStream{Send: stream.Send, CloseSend: stream.CloseSend, Recv: stream.Recv}, nil
}
func (guest *fakeGuest) Close() error { guest.closed = true; return guest.closeError }

type fakeStream struct {
	requests     []*guestagent.ExecRequest
	responses    []*guestagent.ExecResponse
	sendError    error
	sendErrorAt  int
	closeError   error
	receiveError error
	onReceive    func()
	closed       bool
}

func (stream *fakeStream) Send(request *guestagent.ExecRequest) error {
	stream.requests = append(stream.requests, request)
	if stream.sendErrorAt == 0 || stream.sendErrorAt == len(stream.requests) {
		return stream.sendError
	}
	return nil
}
func (stream *fakeStream) CloseSend() error { stream.closed = true; return stream.closeError }
func (stream *fakeStream) Recv() (*guestagent.ExecResponse, error) {
	if stream.onReceive != nil {
		stream.onReceive()
	}
	if len(stream.responses) == 0 {
		if stream.receiveError != nil {
			return nil, stream.receiveError
		}
		return nil, io.EOF
	}
	response := stream.responses[0]
	stream.responses = stream.responses[1:]
	return response, nil
}
func exitResponse(code int32) *guestagent.ExecResponse {
	return &guestagent.ExecResponse{Type: &guestagent.ExecResponse_Exit_{Exit: &guestagent.ExecResponse_Exit{Code: code}}}
}
