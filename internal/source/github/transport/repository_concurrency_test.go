package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
)

func TestRepositoryOperationWaitCanBeCanceledWithoutStartingRequest(t *testing.T) {
	t.Parallel()
	api := &blockedRepositoryAPI{started: make(chan struct{}), release: make(chan struct{})}
	client := clientFromUpstream("https://github.com/acme/widgets", api, &upstreamAPIFake{}, nil)
	first := make(chan error, 1)
	go func() {
		_, err := client.EnsureRunnerPool(t.Context(), "linux", "default")
		first <- err
	}()
	<-api.started
	assertCanceledBootstrapWait(t, client, api)
	close(api.release)
	if err := <-first; err != nil {
		t.Fatalf("first operation: %v", err)
	}
	if _, err := client.GenerateBootstrap(t.Context(), Registration{ID: 29}, "runner"); err != nil {
		t.Fatalf("subsequent operation: %v", err)
	}
}

func assertCanceledBootstrapWait(t *testing.T, client *Client, api *blockedRepositoryAPI) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	waiting := make(chan error, 1)
	go func() {
		_, err := client.GenerateBootstrap(ctx, Registration{ID: 29}, "runner")
		waiting <- err
	}()
	select {
	case err := <-waiting:
		cancel()
		t.Errorf("bootstrap bypassed the active repository operation: %v", err)
		return
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waiting:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("waiting bootstrap error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Error("canceled repository operation kept waiting")
	}
	if api.bootstrapRequests.Load() != 0 {
		t.Error("canceled waiter started an upstream request")
	}
}

func TestCanceledRepositoryOperationsDoNotReachGitHub(t *testing.T) {
	t.Parallel()
	api := &upstreamAPIFake{}
	client := clientFromUpstream("https://github.com/acme/widgets", api, api, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for attempt := 0; attempt < 20; attempt++ {
		_, registrationErr := client.EnsureRunnerPool(ctx, "linux", "default")
		_, groupErr := client.ResolveRunnerGroup(ctx, "default")
		_, bootstrapErr := client.GenerateBootstrap(ctx, Registration{ID: 29}, "runner")
		_, pruneErr := client.PruneRunnerPools(ctx, 17, nil, nil)
		cleanupErr := client.CleanupRunner(ctx, "runner")
		for _, err := range []error{registrationErr, groupErr, bootstrapErr, pruneErr, cleanupErr} {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled repository operation = %v", err)
			}
		}
	}
}

type blockedRepositoryAPI struct {
	upstreamAPIFake
	started           chan struct{}
	release           chan struct{}
	bootstrapRequests atomic.Int64
}

func (api *blockedRepositoryAPI) GetRunnerGroupByName(context.Context, string) (*upstream.RunnerGroup, error) {
	close(api.started)
	<-api.release
	return &upstream.RunnerGroup{ID: 17}, nil
}

func (api *blockedRepositoryAPI) GenerateJitRunnerConfig(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	api.bootstrapRequests.Add(1)
	return &upstream.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "opaque"}, nil
}
