package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type lifecycleProvider struct {
	instances       []RunnerInstance
	listErr         error
	listErrors      []error
	destroyErr      error
	retainOnDestroy bool
}

type terminalLifecycleProvider struct {
	lists      [][]RunnerInstance
	listErr    error
	listErrors []error
	destroyErr error
	destroyed  int
}

func (provider *terminalLifecycleProvider) List(context.Context, string) ([]RunnerInstance, error) {
	if len(provider.listErrors) > 0 {
		err := provider.listErrors[0]
		provider.listErrors = provider.listErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	if len(provider.lists) == 0 {
		return nil, nil
	}
	result := provider.lists[0]
	provider.lists = provider.lists[1:]
	return result, nil
}
func (*terminalLifecycleProvider) Start(context.Context, RunnerStartRequest) error { return nil }
func (*terminalLifecycleProvider) Destroy(context.Context, RunnerInstance) error   { return nil }
func (provider *terminalLifecycleProvider) DestroyTerminal(context.Context, RunnerInstance) error {
	provider.destroyed++
	return provider.destroyErr
}

type lifecycleErrorStore struct {
	*MemoryDemandStore
	inventoryErr error
	reapErr      error
	registerErr  error
	confirmErr   error
}

func (store *lifecycleErrorStore) RunnerLifecycles(ctx context.Context, scaleSet, repository string) ([]RunnerLifecycle, error) {
	if store.inventoryErr != nil {
		return nil, store.inventoryErr
	}
	return store.MemoryDemandStore.RunnerLifecycles(ctx, scaleSet, repository)
}

func (store *lifecycleErrorStore) RequestRunnerReap(
	ctx context.Context, scaleSet, repository string, requestID int64, runnerName, reason string, requestedAt time.Time,
) error {
	if store.reapErr != nil {
		return store.reapErr
	}
	return store.MemoryDemandStore.RequestRunnerReap(ctx, scaleSet, repository, requestID, runnerName, reason, requestedAt)
}

func (store *lifecycleErrorStore) RegisterRunner(ctx context.Context, runner RunnerLifecycle) error {
	if store.registerErr != nil {
		return store.registerErr
	}
	return store.MemoryDemandStore.RegisterRunner(ctx, runner)
}

func (store *lifecycleErrorStore) ConfirmRunnerWaiting(ctx context.Context, runner RunnerLifecycle, at time.Time) error {
	if store.confirmErr != nil {
		return store.confirmErr
	}
	return store.MemoryDemandStore.ConfirmRunnerWaiting(ctx, runner, at)
}

func (provider *lifecycleProvider) List(context.Context, string) ([]RunnerInstance, error) {
	if len(provider.listErrors) > 0 {
		err := provider.listErrors[0]
		provider.listErrors = provider.listErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	return append([]RunnerInstance(nil), provider.instances...), nil
}

func (*lifecycleProvider) Start(context.Context, RunnerStartRequest) error { return nil }

func (provider *lifecycleProvider) Destroy(_ context.Context, instance RunnerInstance) error {
	if provider.destroyErr != nil {
		return provider.destroyErr
	}
	if provider.retainOnDestroy {
		return nil
	}
	for index := range provider.instances {
		if provider.instances[index].ID == instance.ID {
			provider.instances = append(provider.instances[:index], provider.instances[index+1:]...)
			break
		}
	}
	return nil
}

type lifecycleRegistry struct {
	runners           map[string]JITRunner
	findErr           error
	confirmErr        error
	removeErr         error
	retainAfterRemove bool
	findCalls         int
}

func (*lifecycleRegistry) Generate(context.Context, string) (JITRunnerConfig, error) {
	return JITRunnerConfig{}, nil
}

func (registry *lifecycleRegistry) Remove(_ context.Context, runner JITRunner) error {
	if registry.removeErr != nil {
		return registry.removeErr
	}
	if !registry.retainAfterRemove {
		delete(registry.runners, runner.Name)
	}
	return nil
}

func (registry *lifecycleRegistry) Find(_ context.Context, runnerName string) (JITRunner, bool, error) {
	registry.findCalls++
	if registry.findErr != nil {
		return JITRunner{}, false, registry.findErr
	}
	if registry.confirmErr != nil && registry.findCalls > 1 {
		return JITRunner{}, false, registry.confirmErr
	}
	runner, found := registry.runners[runnerName]
	return runner, found, nil
}

func TestReapRunnerLifecycleConfirmsProviderAndGitHubAbsence(t *testing.T) {
	provider := &lifecycleProvider{instances: []RunnerInstance{{ID: "owned", RunnerName: "runner", State: RunnerStateRunning}}}
	registry := &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 7, Name: "runner"}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: provider, JITConfig: registry}}
	reaped, err := reconciler.ReapRunnerLifecycle(context.Background(), RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 7})
	if err != nil || !reaped || len(provider.instances) != 0 || len(registry.runners) != 0 {
		t.Fatalf("reaped=%t err=%v provider=%#v github=%#v", reaped, err, provider.instances, registry.runners)
	}
	reaped, err = reconciler.ReapRunnerLifecycle(context.Background(), RunnerLifecycle{RunnerName: "already-absent"})
	if err != nil || !reaped {
		t.Fatalf("idempotent reap=%t err=%v", reaped, err)
	}
}

type reapRunnerLifecycleCase struct {
	name       string
	provider   *lifecycleProvider
	registry   JITConfigGenerator
	runner     RunnerLifecycle
	wantError  string
	wantReaped bool
}

var reapRunnerLifecycleCases = []reapRunnerLifecycleCase{
	{name: "provider list", provider: &lifecycleProvider{listErr: errors.New("list")}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "list runner instances"},
	{name: "provider destroy", provider: &lifecycleProvider{instances: []RunnerInstance{{ID: "runner", RunnerName: "runner"}}, destroyErr: errors.New("destroy")}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "destroy runner"},
	{name: "provider still present", provider: &lifecycleProvider{instances: []RunnerInstance{{ID: "runner", RunnerName: "runner"}}, retainOnDestroy: true}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, runner: RunnerLifecycle{RunnerName: "runner"}},
	{name: "github inspect", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}, findErr: errors.New("find")}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "inspect github runner"},
	{name: "github remove", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 1, Name: "runner"}}, removeErr: errors.New("remove")}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "remove github runner"},
	{name: "github confirm", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 1, Name: "runner"}}, confirmErr: errors.New("confirm")}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "confirm github runner"},
	{name: "github still present", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 1, Name: "runner"}}, retainAfterRemove: true}, runner: RunnerLifecycle{RunnerName: "runner"}},
	{name: "uninspectable without id", provider: &lifecycleProvider{}, registry: &recordingJITGenerator{}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "absence cannot be confirmed"},
	{name: "uninspectable remove", provider: &lifecycleProvider{}, registry: &recordingJITGenerator{removeErr: errors.New("remove")}, runner: RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 1}, wantError: "remove github runner"},
	{name: "uninspectable success", provider: &lifecycleProvider{}, registry: &recordingJITGenerator{}, runner: RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 1}, wantReaped: true},
}

func TestReapRunnerLifecycleRetainsIncompleteCleanup(t *testing.T) {
	for _, test := range reapRunnerLifecycleCases {
		t.Run(test.name, func(t *testing.T) {
			assertReapRunnerLifecycleCase(t, test)
		})
	}
}

func assertReapRunnerLifecycleCase(t *testing.T, test reapRunnerLifecycleCase) {
	t.Helper()
	reaped, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: test.provider, JITConfig: test.registry}}).
		ReapRunnerLifecycle(context.Background(), test.runner)
	if reaped != test.wantReaped {
		t.Fatalf("reaped=%t", reaped)
	}
	if test.wantError == "" && err != nil {
		t.Fatal(err)
	}
	if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
		t.Fatalf("error=%v", err)
	}
}

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

func TestReconcilerRetainsFailedProviderStartForDurableCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	store := createMemoryDemandStore()
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", MaxRunners: 1,
		ProviderKind: RunnerProviderDocker, Provider: failingProvider{failure: "start"}, JITConfig: &recordingJITGenerator{},
		Lifecycle: store, Now: func() time.Time { return now }}}
	if err := reconciler.StartNamed(ctx, 7); err == nil || !strings.Contains(err.Error(), "start runner") {
		t.Fatalf("error=%v", err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "set", "repo")
	if err != nil || len(pending) != 1 || pending[0].ReapReason != "provider_start_failed" {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}

	failingStore := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), reapErr: errors.New("retain")}
	reconciler.ScaleSet.Lifecycle = failingStore
	if err = reconciler.StartNamed(ctx, 8); err == nil || !strings.Contains(err.Error(), "retain cleanup obligation") {
		t.Fatalf("error=%v", err)
	}
}

func TestReconcilerTerminalReapBoundaries(t *testing.T) {
	instance := RunnerInstance{ID: "id", RunnerName: "runner", State: RunnerStateCompleted}
	tests := []struct {
		name      string
		provider  Provider
		want      bool
		wantError bool
	}{
		{name: "unsupported", provider: &recordingProvider{}, wantError: true},
		{name: "list", provider: &terminalLifecycleProvider{listErr: errors.New("list")}, wantError: true},
		{name: "absent", provider: &terminalLifecycleProvider{}, want: true},
		{name: "destroy", provider: &terminalLifecycleProvider{lists: [][]RunnerInstance{{instance}}, destroyErr: errors.New("destroy")}, wantError: true},
		{name: "retained", provider: &terminalLifecycleProvider{lists: [][]RunnerInstance{{instance}, {instance}}}},
		{name: "removed", provider: &terminalLifecycleProvider{lists: [][]RunnerInstance{{instance}, nil}}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reaped, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: test.provider}}).ReapRunner(context.Background(), "runner")
			if reaped != test.want || (err != nil) != test.wantError {
				t.Fatalf("reaped=%t err=%v", reaped, err)
			}
		})
	}
}

func TestDesiredRunnerCountBoundsCapacity(t *testing.T) {
	if DesiredRunnerCount(-1, 3) != 0 || DesiredRunnerCount(4, 3) != 3 || DesiredRunnerCount(2, 3) != 2 {
		t.Fatal("desired runner count bounds failed")
	}
}

func TestReconcilerReportsReconcileBoundaries(t *testing.T) {
	ctx := context.Background()
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{failure: "list"}}}).Reconcile(ctx, 1); err == nil {
		t.Fatal("expected reconcile list failure")
	}
	cleanupFailure := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{
		failure: "destroy", instances: []RunnerInstance{{RunnerName: "done", State: RunnerStateCompleted}},
	}}}
	if _, err := cleanupFailure.Reconcile(ctx, 1); err == nil {
		t.Fatal("expected reconcile cleanup failure")
	}
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", MaxRunners: 1, Provider: &recordingProvider{}, JITConfig: failingJITGenerator{}}}).Reconcile(ctx, 1); err == nil {
		t.Fatal("expected reconcile start failure")
	}
}

func TestReconcilerReportsDrainBoundaries(t *testing.T) {
	ctx := context.Background()
	cleanupFailure := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{
		failure: "destroy", instances: []RunnerInstance{{RunnerName: "done", State: RunnerStateCompleted}},
	}}}
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{failure: "list"}}}).Drain(ctx); err == nil {
		t.Fatal("expected drain list failure")
	}
	if _, err := cleanupFailure.Drain(ctx); err == nil {
		t.Fatal("expected drain cleanup failure")
	}
	count, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: &recordingProvider{instances: []RunnerInstance{
		{RunnerName: "unknown", State: RunnerStateUnknown}, {RunnerName: "running", State: RunnerStateRunning},
	}}}}).Drain(ctx)
	if err != nil || count != 1 {
		t.Fatalf("drain count=%d err=%v", count, err)
	}
}

func TestReconcilerCoversLifecycleStartAndInspectionFailures(t *testing.T) {
	t.Run("terminal inspection", testTerminalReapInspectionFailure)
	t.Run("lifecycle inspection", testLifecycleReapInspectionFailure)
	t.Run("provider start", testNonDurableProviderStartFailures)
	t.Run("waiting confirmation", testWaitingConfirmationFailure)
	t.Run("registration", testDurableRegistrationFailures)
	t.Run("completed cleanup", testCompletedRunnerCleanup)
}

func testTerminalReapInspectionFailure(t *testing.T) {
	ctx := context.Background()
	instance := RunnerInstance{ID: "id", RunnerName: "runner", State: RunnerStateRunning}
	terminal := &terminalLifecycleProvider{lists: [][]RunnerInstance{{instance}}, listErrors: []error{nil, errors.New("second list")}}
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: terminal}}).ReapRunner(ctx, "runner"); err == nil {
		t.Fatal("expected terminal confirmation inspection failure")
	}
}

func testLifecycleReapInspectionFailure(t *testing.T) {
	ctx := context.Background()
	instance := RunnerInstance{ID: "id", RunnerName: "runner", State: RunnerStateRunning}
	provider := &lifecycleProvider{instances: []RunnerInstance{instance}, listErrors: []error{nil, errors.New("second list")}}
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: provider, JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}}}).
		ReapRunnerLifecycle(ctx, RunnerLifecycle{RunnerName: "runner"}); err == nil {
		t.Fatal("expected lifecycle confirmation inspection failure")
	}
}

func testNonDurableProviderStartFailures(t *testing.T) {
	ctx := context.Background()
	for _, removeErr := range []error{nil, errors.New("remove")} {
		jit := &recordingJITGenerator{removeErr: removeErr}
		err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", MaxRunners: 1, Provider: failingProvider{failure: "start"}, JITConfig: jit}}).StartNamed(ctx, 1)
		if err == nil {
			t.Fatal("expected non-durable start failure")
		}
	}
}

func testWaitingConfirmationFailure(t *testing.T) {
	ctx := context.Background()
	confirmFailure := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), confirmErr: errors.New("confirm")}
	if err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", MaxRunners: 1,
		ProviderKind: RunnerProviderDocker, Provider: &recordingProvider{}, JITConfig: &recordingJITGenerator{}, Lifecycle: confirmFailure}}).
		StartNamed(ctx, 1); err == nil {
		t.Fatal("expected waiting confirmation failure")
	}
}

func testDurableRegistrationFailures(t *testing.T) {
	ctx := context.Background()
	for _, removeErr := range []error{nil, errors.New("remove")} {
		registrationFailure := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), registerErr: errors.New("register")}
		if err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", MaxRunners: 1, Provider: &recordingProvider{},
			JITConfig: &recordingJITGenerator{removeErr: removeErr}, Lifecycle: registrationFailure}}).StartNamed(ctx, 1); err == nil {
			t.Fatal("expected durable registration failure")
		}
	}
}

func testCompletedRunnerCleanup(t *testing.T) {
	ctx := context.Background()
	if count, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: &recordingProvider{instances: []RunnerInstance{
		{RunnerName: "done", State: RunnerStateCompleted}, {RunnerName: "running", State: RunnerStateRunning},
	}}}}).Running(ctx); err != nil || count != 1 {
		t.Fatalf("running count=%d err=%v", count, err)
	}
}

func TestReconcilerFullCapacitySurfacesUsabilityFailure(t *testing.T) {
	store := &lifecycleErrorStore{MemoryDemandStore: createMemoryDemandStore(), inventoryErr: errors.New("inventory")}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Repository: "repo", MaxRunners: 1,
		Provider:  &lifecycleProvider{instances: []RunnerInstance{{RunnerName: "runner", State: RunnerStateRunning}}},
		JITConfig: &lifecycleRegistry{runners: map[string]JITRunner{}}, Lifecycle: store}}
	if err := reconciler.StartOne(context.Background()); err == nil {
		t.Fatal("expected capacity usability failure")
	}
}
