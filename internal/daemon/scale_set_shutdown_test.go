package daemon

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestScaleSetStopPollingWaitsForPollerBeforeShutdown(t *testing.T) {
	ctx, messageClient, client := blockingScaleSetFixture(t)
	scaleSet, err := runTestScaleSet(ctx, slog.Default(), client, config.ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "default",
		MaxRunners:  2,
	}, successfulScaleSetOperations())
	if err != nil {
		t.Fatalf("expected scale set success, got %v", err)
	}

	select {
	case <-messageClient.entered:
	case <-time.After(time.Second):
		t.Fatal("expected poller to enter message receive")
	}
	scaleSet.StopPolling()
	select {
	case <-messageClient.exited:
	default:
		t.Fatal("expected StopPolling to wait for poller exit")
	}
	if err := scaleSet.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected shutdown success, got %v", err)
	}
}

func blockingScaleSetFixture(t *testing.T) (ctx context.Context, messageClient *blockingMessageClient, client *fakeScaleSetClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	messageClient = &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	client = fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 10}, &upstream.RunnerScaleSet{ID: 20},
		scaleSetMessageClient{Client: messageClient},
	)
	client.fakeScaleSetDeletionClient = fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(context.Context, int) error {
		return nil
	}}
	return ctx, messageClient, client
}

func successfulScaleSetOperations() scaleSetSessionOperations {
	return scaleSetSessionOperations{
		RunnerHostname: func() string { return "host" },
		BuildReconciler: func(
			_ context.Context,
			_ string,
			scaleSetConfig config.ScaleSetConfig,
			_ scaleSetJITClient,
			_ int,
		) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
				Name: scaleSetConfig.Name, MaxRunners: scaleSetConfig.MaxRunners,
				Provider: &fakeScalerProvider{}, JITConfig: fakeJITConfigGenerator{},
			}}, nil
		},
		PollRetryDelay: time.Nanosecond,
	}
}

func TestScaleSetHandlerSurfacesReconcileFailure(t *testing.T) {
	handler := scaleSetHandler{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name:       "linux",
		MaxRunners: 1,
		Provider:   &fakeScalerProvider{listErr: errors.New("list failed")},
		JITConfig:  fakeJITConfigGenerator{},
	}}}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	})
	if err == nil {
		t.Fatal("expected handler error")
	}
}
