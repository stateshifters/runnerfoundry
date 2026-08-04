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
	ctx context.Context, scaleSet, repository string, identity RunnerIdentity, reason string, requestedAt time.Time,
) error {
	if store.reapErr != nil {
		return store.reapErr
	}
	return store.MemoryDemandStore.RequestRunnerReap(ctx, scaleSet, repository, identity, reason, requestedAt)
}

func (store *lifecycleErrorStore) BindRunnerIdentity(
	ctx context.Context, scaleSet, repository string, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time,
) error {
	return store.MemoryDemandStore.BindRunnerIdentity(ctx, scaleSet, repository, identity, githubRunnerID, startedAt)
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
