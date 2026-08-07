package transport

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
)

func TestPruneRunnerPoolsDeletesOnlyAbsentRunnerFoundryRegistrations(t *testing.T) {
	t.Parallel()

	registry := &registryAPIFake{scaleSets: []upstream.RunnerScaleSet{
		{ID: 1, Name: "configured", RunnerGroupID: 17},
		{ID: 2, Name: "stale", RunnerGroupID: 17},
		{ID: 3, Name: "foreign", RunnerGroupID: 17},
		{ID: 4, Name: "mutable", RunnerGroupID: 17},
	}}
	client := &Client{registry: registry}

	_, err := client.PruneRunnerPools(context.Background(), 17, []OwnedRunnerPool{
		{ID: 1, Name: "configured", RunnerGroupID: 17},
	}, []OwnedRunnerPool{
		{ID: 1, Name: "configured", RunnerGroupID: 17},
		{ID: 2, Name: "stale", RunnerGroupID: 17},
	})
	if err != nil {
		t.Fatalf("prune runner pools: %v", err)
	}
	if len(registry.deletedIDs) != 1 || registry.deletedIDs[0] != 2 {
		t.Fatalf("deleted IDs = %#v, want [2]", registry.deletedIDs)
	}
}

func TestPruneRunnerPoolsUsesStoredGroupIDAndReportsRemovedOwnership(t *testing.T) {
	t.Parallel()

	registry := &registryAPIFake{scaleSets: []upstream.RunnerScaleSet{{
		ID: 2, Name: "stale", RunnerGroupID: 17,
	}}}
	client := &Client{registry: registry}
	current := []OwnedRunnerPool{{ID: 1, Name: "linux", RunnerGroupID: 23}}
	owned := []OwnedRunnerPool{
		{ID: 2, Name: "stale", RunnerGroupID: 17},
		{ID: 3, Name: "absent", RunnerGroupID: 17},
		{ID: 4, Name: "another-group", RunnerGroupID: 23},
	}

	result, err := client.PruneRunnerPools(context.Background(), 17, current, owned)
	if err != nil {
		t.Fatalf("prune runner pools: %v", err)
	}
	if len(result.Removed) != 2 || result.Removed[0] != owned[0] || result.Removed[1] != owned[1] {
		t.Fatalf("removed ownership = %#v, want first two records", result.Removed)
	}
	if len(registry.deletedIDs) != 1 || registry.deletedIDs[0] != 2 {
		t.Fatalf("deleted IDs = %#v, want [2]", registry.deletedIDs)
	}
}

type registryAPIFake struct {
	scaleSets  []upstream.RunnerScaleSet
	deletedIDs []int
	listErr    error
	deleteErr  error
}

func (registry *registryAPIFake) ListRunnerScaleSets(context.Context, int) ([]upstream.RunnerScaleSet, error) {
	return registry.scaleSets, registry.listErr
}

func (registry *registryAPIFake) DeleteRunnerScaleSet(_ context.Context, runnerScaleSetID int) error {
	registry.deletedIDs = append(registry.deletedIDs, runnerScaleSetID)
	return registry.deleteErr
}

func TestPruneRunnerPoolsReportsConfigurationAndRegistryFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("registry failed")
	owned := upstream.RunnerScaleSet{
		ID: 2, Name: "stale", Labels: []upstream.Label{{Name: "stale", Type: "System"}},
		RunnerGroupID: 17, RunnerSetting: upstream.RunnerSetting{DisableUpdate: true},
	}
	tests := []struct {
		name   string
		client *Client
	}{
		{name: "nil client", client: nil},
		{name: "missing registry", client: &Client{}},
		{name: "list failure", client: &Client{registry: &registryAPIFake{listErr: expectedErr}}},
		{name: "delete failure", client: &Client{registry: &registryAPIFake{
			scaleSets: []upstream.RunnerScaleSet{owned}, deleteErr: expectedErr,
		}}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := testCase.client.PruneRunnerPools(context.Background(), 17, nil, []OwnedRunnerPool{
				{ID: 2, Name: "stale", RunnerGroupID: 17},
			}); err == nil {
				t.Fatal("prune succeeded")
			}
		})
	}
}

func TestPruneRunnerPoolsRetainsMismatchedOwnership(t *testing.T) {
	t.Parallel()

	registry := &registryAPIFake{scaleSets: []upstream.RunnerScaleSet{
		{ID: 1, Name: "renamed", RunnerGroupID: 17},
		{ID: 2, Name: "stale", RunnerGroupID: 23},
	}}
	owned := []OwnedRunnerPool{
		{ID: 1, Name: "stale", RunnerGroupID: 17},
		{ID: 2, Name: "stale", RunnerGroupID: 17},
	}
	result, err := (&Client{registry: registry}).PruneRunnerPools(context.Background(), 17, nil, owned)
	if err != nil {
		t.Fatalf("prune runner pools: %v", err)
	}
	if len(result.Removed) != 0 || len(registry.deletedIDs) != 0 {
		t.Fatalf("mismatched ownership was removed: result=%#v deleted=%#v", result, registry.deletedIDs)
	}
}

func TestPruneRunnerPoolsTreatsDeleteAbsenceAsRemoved(t *testing.T) {
	t.Parallel()

	registry := &registryAPIFake{
		scaleSets: []upstream.RunnerScaleSet{{ID: 2, Name: "stale", RunnerGroupID: 17}},
		deleteErr: upstream.NotFoundError,
	}
	owned := OwnedRunnerPool{ID: 2, Name: "stale", RunnerGroupID: 17}
	result, err := (&Client{registry: registry}).PruneRunnerPools(context.Background(), 17, nil, []OwnedRunnerPool{owned})
	if err != nil || len(result.Removed) != 1 || result.Removed[0] != owned {
		t.Fatalf("prune result=%#v err=%v, want removed absent ownership", result, err)
	}
}
