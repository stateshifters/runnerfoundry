package orchard

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestStartRejectsInvalidOwnershipAndPreservesControllerFailure(t *testing.T) {
	api := &fakeAPI{}
	provider := testProvider(t, api, nil)
	for _, request := range []execution.StartRequest{{RunnerPool: "pool", RunnerName: "foreign"}, {RunnerPool: "", RunnerName: runnerName}, {RunnerPool: core.RunnerPool(strings.Repeat("pool", 100)), RunnerName: runnerName}, {RunnerPool: "pool", RunnerName: runnerName, EndpointID: "foreign"}} {
		if err := provider.Start(context.Background(), request); err == nil {
			t.Fatalf("accepted %+v", request)
		}
	}
	if len(api.created) != 0 {
		t.Fatal("invalid launch reached controller")
	}
	api.createError = errors.New("sensitive response")
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); err == nil || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("create error=%v", err)
	}
}

func TestEachStartUsesIndependentNativeSpecFromConstruction(t *testing.T) {
	spec := []byte(`{"image":"base","runtime":"tart","os":"darwin","arch":"arm64","powerState":"running","restart_policy":"Never","labels":{"region":"west"},"hostDirs":[{"name":"share","path":"/source"}]}`)
	api := &fakeAPI{}
	connector := &fakeConnector{err: errors.New("stop after creation")}
	provider, err := NewProvider(api, connector, Config{ControllerURL: "https://orchard.example", VMSpec: spec})
	if err != nil {
		t.Fatal(err)
	}
	for index := range spec {
		spec[index] = 'x'
	}
	request := execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}
	if err := provider.Start(context.Background(), request); err == nil {
		t.Fatal("failed connection accepted")
	}
	api.created[0].Labels["region"] = "mutated"
	api.created[0].HostDirs[0].Name = "mutated"
	if err := provider.Start(context.Background(), request); err == nil {
		t.Fatal("failed connection accepted")
	}
	if len(api.created) != 2 || api.created[1].Labels["region"] != "west" || api.created[1].HostDirs[0].Name != "share" || api.created[1].Image != "base" || api.created[1].PowerState != "running" || api.created[1].RestartPolicy != "Never" {
		t.Fatalf("spec=%+v", api.created)
	}
}

func TestGuestReadinessRetriesBeforeSingleDelivery(t *testing.T) {
	failed := &fakeStream{responses: []*guestagent.ExecResponse{exitResponse(1)}}
	ready := &fakeStream{responses: []*guestagent.ExecResponse{exitResponse(0)}}
	delivered := &fakeStream{responses: []*guestagent.ExecResponse{exitResponse(0)}}
	guest := &fakeGuest{streams: []*fakeStream{failed, ready, delivered}}
	provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
	ticks := make(chan time.Time, 1)
	ticks <- time.Time{}
	provider.after = func(time.Duration) <-chan time.Time { return ticks }
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); err != nil {
		t.Fatal(err)
	}
	if !guest.closed || !failed.closed || len(delivered.requests) != 1 {
		t.Fatal("readiness did not precede single delivery")
	}
}

func TestGuestCancellationBeforeInitializationClosesConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	guest := &fakeGuest{}
	provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
	if err := provider.Start(ctx, execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); !errors.Is(err, context.Canceled) || !guest.closed {
		t.Fatalf("cancel=%v closed=%v", err, guest.closed)
	}
}

func TestGuestCancellationDuringReadinessStopsInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guest := &fakeGuest{err: errors.New("not ready")}
	provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
	provider.after = func(time.Duration) <-chan time.Time { cancel(); return nil }
	if err := provider.Start(ctx, execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel waiting=%v", err)
	}
}

func TestGuestCancellationAfterDeliveryCannotReportSuccessfulInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guest := &fakeGuest{streams: []*fakeStream{{responses: []*guestagent.ExecResponse{exitResponse(0)}}, {responses: []*guestagent.ExecResponse{exitResponse(0)}, onReceive: cancel}}}
	provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
	if err := provider.Start(ctx, execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delivery=%v", err)
	}
}

func TestGuestConnectionCloseFailurePreventsSuccessfulInitialization(t *testing.T) {
	guest := &fakeGuest{streams: []*fakeStream{{responses: []*guestagent.ExecResponse{exitResponse(0)}}, {responses: []*guestagent.ExecResponse{exitResponse(0)}}}, closeError: errors.New("sensitive transport failure")}
	provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); err == nil || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("guest closure=%v", err)
	}
}

func TestDeliveryFailureIsNeverReplayedAndDoesNotDiscloseOutput(t *testing.T) {
	for _, failure := range []struct {
		name   string
		stream *fakeStream
	}{
		{"command send", &fakeStream{sendError: errors.New("sensitive")}},
		{"input send", &fakeStream{sendErrorAt: 2, sendError: errors.New("sensitive")}},
		{"input close", &fakeStream{closeError: errors.New("sensitive")}},
		{"uncertain EOF", &fakeStream{}},
		{"receive error", &fakeStream{receiveError: errors.New("sensitive")}},
		{"missing result", &fakeStream{responses: []*guestagent.ExecResponse{nil}}},
		{"nonzero result", &fakeStream{responses: []*guestagent.ExecResponse{exitResponse(2)}}},
		{"empty exit", &fakeStream{responses: []*guestagent.ExecResponse{{Type: &guestagent.ExecResponse_Exit_{}}}}},
	} {
		t.Run(failure.name, func(t *testing.T) {
			guest := &fakeGuest{streams: []*fakeStream{{responses: []*guestagent.ExecResponse{exitResponse(0)}}, failure.stream}}
			provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
			err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName, Bootstrap: core.NewBootstrap([]byte("secret"))})
			if err == nil || strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "secret") || !guest.closed || len(guest.streams) != 0 {
				t.Fatalf("delivery error=%v guest=%+v", err, guest)
			}
		})
	}
}

func TestLargeBootstrapIsChunkedAndGuestOutputIsDiscarded(t *testing.T) {
	content := strings.Repeat("b", 70*1024)
	delivery := &fakeStream{responses: []*guestagent.ExecResponse{{Type: &guestagent.ExecResponse_StandardOutput{StandardOutput: &guestagent.IOChunk{Data: []byte("secret")}}}, {Type: &guestagent.ExecResponse_StandardError{StandardError: &guestagent.IOChunk{Data: []byte("secret")}}}, exitResponse(0)}}
	guest := &fakeGuest{streams: []*fakeStream{{responses: []*guestagent.ExecResponse{exitResponse(0)}}, delivery}}
	provider := testProvider(t, &fakeAPI{}, &fakeConnector{guest: guest})
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName, Bootstrap: core.NewBootstrap([]byte(content))}); err != nil {
		t.Fatal(err)
	}
	received := ""
	for _, request := range delivery.requests[1:] {
		chunk := request.GetStandardInput().Data
		if len(chunk) > 32*1024 {
			t.Fatal("unbounded message")
		}
		received += string(chunk)
	}
	if received != content {
		t.Fatal("bootstrap truncated")
	}
}
