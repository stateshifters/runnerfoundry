package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAssignedRunnerPresentRequiresDurableAssignmentIdentity(t *testing.T) {
	present, err := (Reconciler{}).AssignedRunnerPresent(context.Background())
	if err == nil || present || !strings.Contains(err.Error(), "durable assignment ID") {
		t.Fatalf("present = %t, error = %v", present, err)
	}
}

func TestAssignedRunnerPresentFindsAssignmentRunner(t *testing.T) {
	provider := &lifecycleProvider{instances: []RunnerInstance{{
		RunnerName: runnerName("linux", 17), State: RunnerStateRunning,
	}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "linux", AssignmentID: 17, Provider: provider,
	}}

	present, err := reconciler.AssignedRunnerPresent(context.Background())
	if err != nil || !present {
		t.Fatalf("present = %t, error = %v", present, err)
	}
}

func TestAssignedRunnerPresentReportsProviderFailure(t *testing.T) {
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "linux", AssignmentID: 17, Provider: &lifecycleProvider{listErr: errors.New("provider")},
	}}

	present, err := reconciler.AssignedRunnerPresent(context.Background())
	if err == nil || present || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("present = %t, error = %v", present, err)
	}
}

func TestStartOneUsesDurableAssignmentRunnerName(t *testing.T) {
	provider := &recordingProvider{}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "linux", AssignmentID: 17, MaxRunners: 1,
		Provider: provider, JITConfig: &recordingJITGenerator{},
	}}

	if err := reconciler.StartOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(provider.created) != 1 || provider.created[0].RunnerName != runnerName("linux", 17) {
		t.Fatalf("started runners = %#v", provider.created)
	}
}

func TestStartOneRetainsRunningDurableAssignmentRunner(t *testing.T) {
	expectedRunnerName := runnerName("linux", 17)
	provider := &recordingProvider{instances: []RunnerInstance{{
		RunnerName: expectedRunnerName, State: RunnerStateRunning,
	}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "linux", AssignmentID: 17, MaxRunners: 1,
		Provider: provider, JITConfig: &recordingJITGenerator{},
	}}

	if err := reconciler.StartOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(provider.created) != 0 {
		t.Fatalf("duplicate runners started = %#v", provider.created)
	}
}
