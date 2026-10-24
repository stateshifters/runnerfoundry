package github_test

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestPollOnceValidatesDependenciesAndTransportResults(t *testing.T) {
	t.Parallel()

	tests := map[string]githubsource.Adapter{
		"missing client": {
			SourceEvents: &recordingEventHandler{}, AssignedDemand: &recordingEventHandler{},
		},
		"missing handler": {
			Client: &recordingClient{}, AssignedDemand: &recordingEventHandler{},
		},
		"missing assigned demand": {Client: &recordingClient{}, SourceEvents: &recordingEventHandler{}},
		"receive failure": {
			Client: &recordingClient{receiveErr: errors.New("receive")}, SourceEvents: &recordingEventHandler{},
			AssignedDemand: &recordingEventHandler{},
		},
		"ambiguous nil": {
			Client: ambiguousNilClient{}, SourceEvents: &recordingEventHandler{},
			AssignedDemand: &recordingEventHandler{},
		},
	}
	for testName, adapter := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			if _, err := adapter.PollOnce(context.Background(), 0); err == nil {
				t.Fatal("PollOnce() error = nil, want failure")
			}
		})
	}
}

func TestPollOnceTreatsNoMessageAsSuccess(t *testing.T) {
	t.Parallel()

	adapter := newAdapter(&recordingClient{}, &recordingEventHandler{})
	received, err := adapter.PollOnce(context.Background(), 0)
	if err != nil || received {
		t.Fatalf("empty poll = (%t, %v), want (false, nil)", received, err)
	}
}

type ambiguousNilClient struct {
	message *upstream.RunnerScaleSetMessage
	err     error
}

func (client ambiguousNilClient) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	return client.message, client.err
}

func (ambiguousNilClient) AcquireRequests(context.Context, []int64) ([]int64, error) {
	return nil, nil
}

func (ambiguousNilClient) DeleteMessage(context.Context, int) error { return nil }
