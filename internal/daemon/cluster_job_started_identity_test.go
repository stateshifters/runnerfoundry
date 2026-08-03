package daemon

import (
	"context"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterMessageHandlerMarksIdentityFreeRunnerBusyByName(t *testing.T) {
	handler, state := busyClusterMessageFixture(t)
	identityFreeRunner := scaler.RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-assigned-1", Phase: scaler.RunnerWaitingForJob,
	}
	if err := state.store.RegisterRunner(context.Background(), identityFreeRunner); err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:         &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: "linux-assigned-1", RunnerID: 100}},
	}); err != nil {
		t.Fatal(err)
	}
	assertIdentityFreeRunnerBusy(t, state)
}

func assertIdentityFreeRunnerBusy(t *testing.T, state *lifecycleClusterState) {
	t.Helper()
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	for _, lifecycle := range lifecycles {
		if lifecycle.RunnerName != "linux-assigned-1" {
			continue
		}
		if lifecycle.Phase != scaler.RunnerBusy || lifecycle.RequestID != 0 {
			t.Fatalf("identity-free lifecycle = %#v", lifecycle)
		}
		return
	}
	t.Fatalf("identity-free lifecycle not found: %#v", lifecycles)
}
