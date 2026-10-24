package transport

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
)

func TestRegistrationRequestsRemainContiguousWhenBootstrapIsWaiting(t *testing.T) {
	t.Parallel()
	api := &registrationSequenceAPI{groupStarted: make(chan struct{}), releaseGroup: make(chan struct{})}
	client := clientFromUpstream("https://github.com/acme/widgets", api, &upstreamAPIFake{}, nil)
	registration := make(chan error, 1)
	go func() {
		_, err := client.EnsureRunnerPool(t.Context(), "linux", "default")
		registration <- err
	}()
	<-api.groupStarted
	bootstrap := make(chan error, 1)
	go func() {
		_, err := client.GenerateBootstrap(t.Context(), Registration{ID: 29}, "runner")
		bootstrap <- err
	}()
	assertBootstrapIsWaiting(t, bootstrap)
	close(api.releaseGroup)
	for _, completed := range []<-chan error{registration, bootstrap} {
		if err := <-completed; err != nil {
			t.Fatalf("repository operation: %v", err)
		}
	}
	if !slices.Equal(api.requests, []string{"group", "lookup", "create", "bootstrap"}) {
		t.Fatalf("repository request sequence = %v", api.requests)
	}
}

func assertBootstrapIsWaiting(t *testing.T, completed chan error) {
	t.Helper()
	select {
	case err := <-completed:
		completed <- err
		t.Errorf("bootstrap started before registration completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
}

type registrationSequenceAPI struct {
	upstreamAPIFake
	mutex        sync.Mutex
	requests     []string
	groupStarted chan struct{}
	releaseGroup chan struct{}
}

func (api *registrationSequenceAPI) record(request string) {
	api.mutex.Lock()
	defer api.mutex.Unlock()
	api.requests = append(api.requests, request)
}

func (api *registrationSequenceAPI) GetRunnerGroupByName(context.Context, string) (*upstream.RunnerGroup, error) {
	api.record("group")
	close(api.groupStarted)
	<-api.releaseGroup
	return &upstream.RunnerGroup{ID: 17}, nil
}

func (api *registrationSequenceAPI) GetRunnerScaleSet(ctx context.Context, group int, name string) (*upstream.RunnerScaleSet, error) {
	api.record("lookup")
	return api.actionsAPIFake.GetRunnerScaleSet(ctx, group, name)
}

func (api *registrationSequenceAPI) CreateRunnerScaleSet(_ context.Context, scaleSet *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error) {
	api.record("create")
	return scaleSet, nil
}

func (api *registrationSequenceAPI) GenerateJitRunnerConfig(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	api.record("bootstrap")
	return &upstream.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "opaque"}, nil
}
