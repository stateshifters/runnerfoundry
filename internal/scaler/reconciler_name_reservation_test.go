package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReconcilerReservesProviderAbsentLifecycleRunnerName(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	if err := store.RegisterRunner(ctx, RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", RunnerName: "set-1", Phase: RunnerReapPending,
	}); err != nil {
		t.Fatal(err)
	}
	provider := &recordingProvider{}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", Repository: "repo", MaxRunners: 2, ProviderKind: RunnerProviderDocker,
		Provider: provider, JITConfig: &recordingJITGenerator{}, Lifecycle: store,
	}}

	if err := reconciler.StartOne(ctx); err != nil {
		t.Fatal(err)
	}
	if len(provider.created) != 1 || provider.created[0].RunnerName != "set-2" {
		t.Fatalf("created=%#v", provider.created)
	}
}

func TestReconcilerReusesRunnerNameAfterConfirmedCleanup(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	reaped := RunnerLifecycle{ScaleSet: "set", Repository: "repo-a", RunnerName: "set-1", Phase: RunnerReapPending}
	if err := store.RegisterRunner(ctx, reaped); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRunnerReaped(ctx, reaped); err != nil {
		t.Fatal(err)
	}
	provider := &recordingProvider{}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", Repository: "repo-b", MaxRunners: 1, ProviderKind: RunnerProviderDocker,
		Provider: provider, JITConfig: &recordingJITGenerator{}, Lifecycle: store,
	}}

	if err := reconciler.StartOne(ctx); err != nil {
		t.Fatal(err)
	}
	if len(provider.created) != 1 || provider.created[0].RunnerName != "set-1" {
		t.Fatalf("created=%#v", provider.created)
	}
}

func TestReconcilerReportsReservedRunnerNameInventoryFailure(t *testing.T) {
	store := &lifecycleErrorStore{MemoryDemandStore: NewMemoryDemandStore(), inventoryErr: errors.New("inventory")}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", Repository: "repo", MaxRunners: 1, Provider: &recordingProvider{},
		JITConfig: &recordingJITGenerator{}, Lifecycle: store,
	}}
	if err := reconciler.StartOne(context.Background()); err == nil || !strings.Contains(err.Error(), "load reserved runner names") {
		t.Fatalf("start error=%v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "load reserved runner names") {
		t.Fatalf("reconcile error=%v", err)
	}
}
