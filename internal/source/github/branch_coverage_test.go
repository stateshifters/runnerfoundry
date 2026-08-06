package github

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestValidateAvailablePreservesFirstOccurrenceAndOrder(t *testing.T) {
	t.Parallel()

	firstQueueTime := time.Date(2026, time.August, 6, 9, 0, 0, 0, time.UTC)
	adapter := &Adapter{Repository: "octo/repo", RunnerPool: core.RunnerPool("linux")}
	events := []*upstream.JobAvailable{
		{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 41, QueueTime: firstQueueTime}},
		{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: firstQueueTime.Add(time.Minute)}},
		{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 41, QueueTime: firstQueueTime.Add(time.Hour)}},
	}

	available, err := adapter.validateAvailable(events)
	if err != nil {
		t.Fatalf("validate available events: %v", err)
	}
	if len(available) != 2 {
		t.Fatalf("available events = %#v, want two unique requests", available)
	}
	if available[0].SourceID != 41 || available[0].JobRequest.EnqueuedAt != firstQueueTime {
		t.Fatalf("first available event = %#v, want first occurrence of request 41", available[0])
	}
	if available[1].SourceID != 42 {
		t.Fatalf("second available event = %#v, want request 42", available[1])
	}
}

func TestValidateAvailableRejectsInvalidEvents(t *testing.T) {
	t.Parallel()

	adapter := &Adapter{Repository: "octo/repo", RunnerPool: core.RunnerPool("linux")}
	for testName, events := range map[string][]*upstream.JobAvailable{
		"nil event":       {nil},
		"invalid request": {{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 0}}},
	} {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			available, err := adapter.validateAvailable(events)
			if err == nil || available != nil {
				t.Fatalf("validateAvailable() = (%#v, %v), want nil and an error", available, err)
			}
		})
	}
}

func TestValidateAvailableAcceptsNoEvents(t *testing.T) {
	t.Parallel()

	adapter := &Adapter{Repository: "octo/repo", RunnerPool: core.RunnerPool("linux")}
	available, err := adapter.validateAvailable(nil)
	if err != nil {
		t.Fatalf("validate no available events: %v", err)
	}
	if len(available) != 0 {
		t.Fatalf("available events = %#v, want empty", available)
	}
}

type pruneCall struct {
	runnerGroup string
	desired     []string
	owned       []transport.OwnedRunnerPool
}

func TestPruneRepositoryGroupsDesiredPoolsAndFiltersOwnership(t *testing.T) {
	t.Parallel()

	var calls []pruneCall
	client := ManagerClient{PruneRunnerPools: func(
		_ context.Context,
		runnerGroup string,
		desired []string,
		owned []transport.OwnedRunnerPool,
	) error {
		calls = append(calls, pruneCall{runnerGroup: runnerGroup, desired: desired, owned: owned})
		return nil
	}}
	configs := []RunnerPoolConfig{
		{RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default"},
		{RunnerPool: core.RunnerPool("windows"), RunnerGroup: "default"},
		{RunnerPool: core.RunnerPool("gpu"), RunnerGroup: "accelerated"},
	}
	existing := []Registration{
		{RepositoryURL: "octo/repo", RunnerPool: core.RunnerPool("old"), RunnerGroupID: 7, ScaleSetID: 71},
		{RepositoryURL: "octo/repo", RunnerPool: core.RunnerPool("older"), RunnerGroupID: 8, ScaleSetID: 81},
		{RepositoryURL: "octo/other", RunnerPool: core.RunnerPool("foreign"), RunnerGroupID: 9, ScaleSetID: 91},
	}

	err := pruneRepository(context.Background(), client, "octo/repo", configs, existing)
	if err != nil {
		t.Fatalf("prune repository: %v", err)
	}
	assertPruneCalls(t, calls)
}

func assertPruneCalls(t *testing.T, calls []pruneCall) {
	t.Helper()
	if len(calls) != 2 {
		t.Fatalf("prune calls = %#v, want two runner groups", calls)
	}
	desiredByGroup := make(map[string][]string, len(calls))
	for _, call := range calls {
		desiredByGroup[call.runnerGroup] = call.desired
		wantOwned := map[int]transport.OwnedRunnerPool{
			71: {ID: 71, Name: "old", RunnerGroupID: 7},
			81: {ID: 81, Name: "older", RunnerGroupID: 8},
		}
		if !reflect.DeepEqual(ownedByID(call.owned), wantOwned) {
			t.Fatalf("owned pools for group %q = %#v, want %#v", call.runnerGroup, call.owned, wantOwned)
		}
	}
	wantDesired := map[string][]string{"default": {"linux", "windows"}, "accelerated": {"gpu"}}
	if !reflect.DeepEqual(desiredByGroup, wantDesired) {
		t.Fatalf("desired pools = %#v, want %#v", desiredByGroup, wantDesired)
	}
}

func ownedByID(owned []transport.OwnedRunnerPool) map[int]transport.OwnedRunnerPool {
	indexed := make(map[int]transport.OwnedRunnerPool, len(owned))
	for _, runnerPool := range owned {
		indexed[runnerPool.ID] = runnerPool
	}
	return indexed
}

func TestPruneRepositoryHandlesEmptyConfigurationAndFailure(t *testing.T) {
	t.Parallel()

	if err := pruneRepository(context.Background(), ManagerClient{}, "octo/repo", nil, nil); err != nil {
		t.Fatalf("prune empty configuration: %v", err)
	}

	wantErr := errors.New("prune failed")
	client := ManagerClient{PruneRunnerPools: func(
		context.Context,
		string,
		[]string,
		[]transport.OwnedRunnerPool,
	) error {
		return wantErr
	}}
	configs := []RunnerPoolConfig{{RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default"}}
	if err := pruneRepository(context.Background(), client, "octo/repo", configs, nil); !errors.Is(err, wantErr) {
		t.Fatalf("prune error = %v, want %v", err, wantErr)
	}
}
