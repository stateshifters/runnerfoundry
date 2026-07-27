package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReconcileRunnerUsabilityReapsOrphansAndAdoptsInstances(t *testing.T) {
	ctx := context.Background()
	reconciler, store := usableRunnerReconciler(t)
	changed, err := reconciler.ReconcileRunnerUsability(ctx)
	if err != nil || changed != 2 {
		t.Fatalf("changed=%d err=%v", changed, err)
	}
	assertUsabilityLifecyclePhases(t, store)
	present, err := reconciler.RunnerPresent(ctx, "adopted")
	if err != nil || !present {
		t.Fatalf("present=%t err=%v", present, err)
	}
}

func TestReconcileRunnerUsabilityRetainsBusyRunnerUntilExactProviderAbsence(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	seedUsabilityLifecycles(t, store, []RunnerLifecycle{{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "busy", Phase: RunnerBusy,
	}})
	provider := &lifecycleProvider{instances: []RunnerInstance{{RunnerName: "busy", State: RunnerStateRunning}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", Repository: "repo", ProviderKind: RunnerProviderDocker,
		Provider: provider, JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}, Lifecycle: store,
	}}
	if changed, err := reconciler.ReconcileRunnerUsability(ctx); err != nil || changed != 0 {
		t.Fatalf("busy runner present: changed=%d err=%v", changed, err)
	}
	provider.instances = nil
	if changed, err := reconciler.ReconcileRunnerUsability(ctx); err != nil || changed != 1 {
		t.Fatalf("busy runner absent: changed=%d err=%v", changed, err)
	}
	lifecycles, err := store.RunnerLifecycles(ctx, "set", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Phase != RunnerReapPending ||
		lifecycles[0].ReapReason != "provider_instance_absent" {
		t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
	}
}

func TestReconcileRunnerUsabilityReportsAbsentBusyRunnerReapFailure(t *testing.T) {
	ctx := context.Background()
	store := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), reapErr: errors.New("reap")}
	seedUsabilityLifecycles(t, store.MemoryDemandStore, []RunnerLifecycle{{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "busy", Phase: RunnerBusy,
	}})
	_, err := (Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", Repository: "repo", Provider: &lifecycleProvider{}, ProviderKind: RunnerProviderDocker,
		JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}, Lifecycle: store,
	}}).ReconcileRunnerUsability(ctx)
	if err == nil || !strings.Contains(err.Error(), "request absent busy runner reap") {
		t.Fatalf("error=%v", err)
	}
}

func usableRunnerReconciler(t *testing.T) (Reconciler, *MemoryDemandStore) {
	t.Helper()
	store := createMemoryDemandStore()
	now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	seedUsabilityLifecycles(t, store, []RunnerLifecycle{
		{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "registered", Phase: RunnerWaitingForJob},
		{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "orphan", Phase: RunnerWaitingForJob},
		{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "busy", Phase: RunnerBusy},
	})
	provider := &lifecycleProvider{instances: []RunnerInstance{
		{ID: "registered", RunnerName: "registered"}, {ID: "orphan", RunnerName: "orphan"}, {ID: "busy", RunnerName: "busy"},
		{ID: "adopted", RunnerName: "adopted"}, {ID: "absent", RunnerName: "absent"}, {ID: "unnamed"},
	}}
	registry := &lifecycleRegistry{runners: map[string]JITRunner{
		"registered": {ID: 1, Name: "registered"}, "adopted": {ID: 2, Name: "adopted"},
	}}
	return Reconciler{ScaleSet: ScaleSetRuntime{Name: "provider-set", LifecycleScaleSet: "set", Repository: "repo",
		ProviderKind: RunnerProviderDocker, Provider: provider, JITConfig: registry, Lifecycle: store, Now: func() time.Time { return now }}}, store
}

func assertUsabilityLifecyclePhases(t *testing.T, store *MemoryDemandStore) {
	t.Helper()
	lifecycles, err := store.RunnerLifecycles(context.Background(), "set", "repo")
	if err != nil {
		t.Fatal(err)
	}
	phases := lifecyclePhases(lifecycles)
	if phases["orphan"] != RunnerReapPending || phases["absent"] != RunnerReapPending || phases["adopted"] != RunnerBusy {
		t.Fatalf("phases=%#v", phases)
	}
}

func seedUsabilityLifecycles(t *testing.T, store *MemoryDemandStore, runners []RunnerLifecycle) {
	t.Helper()
	for _, runner := range runners {
		if err := store.RegisterRunner(context.Background(), runner); err != nil {
			t.Fatal(err)
		}
	}
}

func lifecyclePhases(runners []RunnerLifecycle) map[string]RunnerLifecyclePhase {
	phases := make(map[string]RunnerLifecyclePhase, len(runners))
	for _, runner := range runners {
		phases[runner.RunnerName] = runner.Phase
	}
	return phases
}

func TestReconcileRunnerUsabilityReportsBoundaries(t *testing.T) {
	t.Run("unsupported", testUnsupportedUsabilityReconciliation)
	t.Run("provider", testProviderUsabilityFailures)
	t.Run("waiting", testWaitingRunnerInspectionFailure)
	t.Run("inventory", testLifecycleInventoryFailure)
	t.Run("reap", testOrphanReapRequestFailure)
	t.Run("registration", testAdoptedRunnerRegistrationFailure)
}

func testUnsupportedUsabilityReconciliation(t *testing.T) {
	ctx := context.Background()
	if changed, err := (Reconciler{ScaleSet: ScaleSetRuntime{Lifecycle: &recordingRunnerLifecycleStore{events: &[]string{}}, JITConfig: &recordingJITGenerator{}}}).ReconcileRunnerUsability(ctx); err != nil || changed != 0 {
		t.Fatalf("unsupported changed=%d err=%v", changed, err)
	}
}

func testProviderUsabilityFailures(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		store     *MemoryDemandStore
		provider  *lifecycleProvider
		registry  *lifecycleRegistry
		wantError string
	}{
		{name: "provider list", store: createMemoryDemandStore(), provider: &lifecycleProvider{listErr: errors.New("list")}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, wantError: "list provider instances"},
		{name: "adopt inspect", store: createMemoryDemandStore(), provider: &lifecycleProvider{instances: []RunnerInstance{{RunnerName: "new"}}}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}, findErr: errors.New("find")}, wantError: "inspect adopted github runner"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", Provider: test.provider,
				ProviderKind: RunnerProviderDocker, JITConfig: test.registry, Lifecycle: test.store}}).ReconcileRunnerUsability(ctx)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func testWaitingRunnerInspectionFailure(t *testing.T) {
	ctx := context.Background()
	waitingStore := createMemoryDemandStore()
	if err := waitingStore.RegisterRunner(ctx, RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "waiting", Phase: RunnerWaitingForJob}); err != nil {
		t.Fatal(err)
	}
	_, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", Provider: &lifecycleProvider{},
		JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}, findErr: errors.New("find")}, Lifecycle: waitingStore}}).ReconcileRunnerUsability(ctx)
	if err == nil || !strings.Contains(err.Error(), "inspect waiting github runner") {
		t.Fatalf("error=%v", err)
	}
}

func testLifecycleInventoryFailure(t *testing.T) {
	ctx := context.Background()
	inventoryFailure := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), inventoryErr: errors.New("inventory")}
	_, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", Provider: &lifecycleProvider{},
		JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}, Lifecycle: inventoryFailure}}).ReconcileRunnerUsability(ctx)
	if err == nil || !strings.Contains(err.Error(), "load runner lifecycle inventory") {
		t.Fatalf("error=%v", err)
	}
}

func testOrphanReapRequestFailure(t *testing.T) {
	ctx := context.Background()
	reapFailure := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), reapErr: errors.New("reap")}
	if err := reapFailure.MemoryDemandStore.RegisterRunner(ctx, RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "waiting", Phase: RunnerWaitingForJob}); err != nil {
		t.Fatal(err)
	}
	_, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", Provider: &lifecycleProvider{},
		JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}, Lifecycle: reapFailure}}).ReconcileRunnerUsability(ctx)
	if err == nil || !strings.Contains(err.Error(), "request orphaned runner reap") {
		t.Fatalf("error=%v", err)
	}
}

func testAdoptedRunnerRegistrationFailure(t *testing.T) {
	ctx := context.Background()
	registerFailure := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), registerErr: errors.New("register")}
	_, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", Provider: &lifecycleProvider{instances: []RunnerInstance{{RunnerName: "new"}}},
		JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}, Lifecycle: registerFailure}}).ReconcileRunnerUsability(ctx)
	if err == nil || !strings.Contains(err.Error(), "adopt provider runner") {
		t.Fatalf("error=%v", err)
	}
}
