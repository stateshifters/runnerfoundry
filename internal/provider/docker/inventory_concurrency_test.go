package docker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInventoryPublishesCompleteDefensiveSnapshotsDuringConcurrentRefresh(t *testing.T) {
	inventory := NewInventory(&concurrentInventoryCommands{}, "member")
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() { exerciseConcurrentDockerSnapshots(t, inventory) })
	}
	workers.Wait()
}

func exerciseConcurrentDockerSnapshots(t *testing.T, inventory Inventory) {
	t.Helper()
	for range 20 {
		if err := inventory.Refresh(context.Background()); err != nil {
			t.Error(err)
			return
		}
		instances, err := inventory.Inspect(context.Background())
		if err != nil || len(instances) != 2 {
			t.Errorf("incomplete snapshot=%+v error=%v", instances, err)
			return
		}
		if instances[0].RunnerPool != instances[1].RunnerPool {
			t.Errorf("mixed snapshot=%+v", instances)
			return
		}
		instances[0].RunnerPool = "changed-by-caller"
	}
}

type concurrentInventoryCommands struct {
	dockerInventoryCommands
	version atomic.Int64
}

func (commands *concurrentInventoryCommands) listComposeRunners(context.Context) (string, error) {
	version := commands.version.Add(1)
	return fmt.Sprintf(`{"ID":"first-%d","State":"running","RunnerPool":"pool-%d","RunnerName":"first-%d"}
{"ID":"second-%d","State":"running","RunnerPool":"pool-%d","RunnerName":"second-%d"}`, version, version, version, version, version, version), nil
}
