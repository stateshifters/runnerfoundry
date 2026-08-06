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
		{ID: 1, Name: "configured", Labels: []upstream.Label{{Name: "configured", Type: "System"}}, RunnerSetting: upstream.RunnerSetting{DisableUpdate: true}},
		{ID: 2, Name: "stale", Labels: []upstream.Label{{Name: "stale", Type: "System"}}, RunnerSetting: upstream.RunnerSetting{DisableUpdate: true}},
		{ID: 3, Name: "foreign", Labels: []upstream.Label{{Name: "custom", Type: "User"}}, RunnerSetting: upstream.RunnerSetting{DisableUpdate: true}},
		{ID: 4, Name: "mutable", Labels: []upstream.Label{{Name: "mutable", Type: "System"}}},
	}}
	client := &Client{
		registrations: &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}},
		registry:      registry,
	}

	err := client.PruneRunnerPools(context.Background(), "default", []string{"configured"}, []OwnedRunnerPool{
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
		RunnerSetting: upstream.RunnerSetting{DisableUpdate: true},
	}
	tests := []*Client{
		nil,
		{registrations: &actionsAPIFake{}, registry: &registryAPIFake{}},
		{registrations: &actionsAPIFake{runnerGroupErr: expectedErr}, registry: &registryAPIFake{}},
		{registrations: &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}}, registry: &registryAPIFake{listErr: expectedErr}},
		{registrations: &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}}, registry: &registryAPIFake{scaleSets: []upstream.RunnerScaleSet{owned}, deleteErr: expectedErr}},
	}
	for index, client := range tests {
		if err := client.PruneRunnerPools(context.Background(), "default", nil, []OwnedRunnerPool{
			{ID: 2, Name: "stale", RunnerGroupID: 17},
		}); err == nil {
			t.Fatalf("case %d succeeded", index)
		}
	}
}
