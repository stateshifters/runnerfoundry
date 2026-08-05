package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterRegistrationRejectsRequestOwnedByAnotherRunnerName(t *testing.T) {
	runner := testRunnerLifecycle()
	runner.Phase = scaler.RunnerProvisioning
	state := &hermeticClusterLifecycleState{mirrorOwner: "other-runner"}

	err := storeWithConnection(state.connection(runner)).RegisterRunner(context.Background(), runner)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	if state.generic != nil || state.mirror != nil {
		t.Fatalf("request ownership conflict mutated state: generic=%#v mirror=%#v", state.generic, state.mirror)
	}
}

func TestClusterRegistrationRejectsRequestOwnedByGenericDockerRunner(t *testing.T) {
	runner := testRunnerLifecycle()
	runner.Provider = scaler.RunnerProviderDocker
	runner.Phase = scaler.RunnerProvisioning
	state := &hermeticClusterLifecycleState{genericOwner: "docker-owner"}

	err := storeWithConnection(state.connection(runner)).RegisterRunner(context.Background(), runner)
	if !errors.Is(err, scaler.ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	if state.generic != nil || state.mirror != nil {
		t.Fatalf("generic ownership conflict mutated state: generic=%#v mirror=%#v", state.generic, state.mirror)
	}
}
