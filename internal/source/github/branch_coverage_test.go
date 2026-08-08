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

func TestValidateAvailableSkipsNonActionableEvidence(t *testing.T) {
	t.Parallel()

	adapter := &Adapter{Repository: "octo/repo", RunnerPool: core.RunnerPool("linux")}
	for testName, events := range map[string][]*upstream.JobAvailable{
		"nil event":       {nil},
		"invalid request": {{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 0}}},
	} {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			available, err := adapter.validateAvailable(events)
			if err != nil || len(available) != 0 {
				t.Fatalf("validateAvailable() = (%#v, %v), want empty success", available, err)
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
	runnerGroup transport.RunnerGroupID
	current     []transport.OwnedRunnerPool
	owned       []transport.OwnedRunnerPool
}

func TestPruneRepositoryGroupsDesiredPoolsAndFiltersOwnership(t *testing.T) {
	t.Parallel()

	var calls []pruneCall
	client := ManagerClient{PruneRunnerPools: func(
		_ context.Context,
		runnerGroup transport.RunnerGroupID,
		current []transport.OwnedRunnerPool,
		owned []transport.OwnedRunnerPool,
	) (transport.PruneResult, error) {
		calls = append(calls, pruneCall{runnerGroup: runnerGroup, current: current, owned: owned})
		return transport.PruneResult{}, nil
	}}
	active := []Registration{
		{RepositoryURL: "octo/repo", RunnerPool: core.RunnerPool("linux"), RunnerGroupID: 7, ScaleSetID: 70},
		{RepositoryURL: "octo/repo", RunnerPool: core.RunnerPool("gpu"), RunnerGroupID: 8, ScaleSetID: 80},
	}
	owned := []Registration{
		{RepositoryURL: "octo/repo", RunnerPool: core.RunnerPool("old"), RunnerGroupID: 7, ScaleSetID: 71},
		{RepositoryURL: "octo/repo", RunnerPool: core.RunnerPool("older"), RunnerGroupID: 8, ScaleSetID: 81},
	}

	_, err := pruneRepository(context.Background(), client, active, owned)
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
	currentByGroup := make(map[transport.RunnerGroupID][]transport.OwnedRunnerPool, len(calls))
	for _, call := range calls {
		currentByGroup[call.runnerGroup] = call.current
		wantOwned := map[transport.ScaleSetID]transport.OwnedRunnerPool{}
		if call.runnerGroup == 7 {
			wantOwned[71] = transport.OwnedRunnerPool{ID: 71, Name: "old", RunnerGroupID: 7}
		} else {
			wantOwned[81] = transport.OwnedRunnerPool{ID: 81, Name: "older", RunnerGroupID: 8}
		}
		if !reflect.DeepEqual(ownedByID(call.owned), wantOwned) {
			t.Fatalf("owned pools for group %d = %#v, want %#v", call.runnerGroup, call.owned, wantOwned)
		}
	}
	wantCurrent := map[transport.RunnerGroupID][]transport.OwnedRunnerPool{
		7: {{ID: 70, Name: "linux", RunnerGroupID: 7}},
		8: {{ID: 80, Name: "gpu", RunnerGroupID: 8}},
	}
	if !reflect.DeepEqual(currentByGroup, wantCurrent) {
		t.Fatalf("current pools = %#v, want %#v", currentByGroup, wantCurrent)
	}
}

func ownedByID(owned []transport.OwnedRunnerPool) map[transport.ScaleSetID]transport.OwnedRunnerPool {
	indexed := make(map[transport.ScaleSetID]transport.OwnedRunnerPool, len(owned))
	for _, runnerPool := range owned {
		indexed[runnerPool.ID] = runnerPool
	}
	return indexed
}

func TestPruneRepositoryHandlesEmptyConfigurationAndFailure(t *testing.T) {
	t.Parallel()

	if _, err := pruneRepository(context.Background(), ManagerClient{}, nil, nil); err != nil {
		t.Fatalf("prune empty configuration: %v", err)
	}

	wantErr := errors.New("prune failed")
	client := ManagerClient{PruneRunnerPools: func(
		context.Context,
		transport.RunnerGroupID,
		[]transport.OwnedRunnerPool,
		[]transport.OwnedRunnerPool,
	) (transport.PruneResult, error) {
		return transport.PruneResult{}, wantErr
	}}
	active := []Registration{{RunnerPool: core.RunnerPool("linux"), RunnerGroupID: 7, ScaleSetID: 70}}
	if _, err := pruneRepository(context.Background(), client, active, nil); !errors.Is(err, wantErr) {
		t.Fatalf("prune error = %v, want %v", err, wantErr)
	}
}
