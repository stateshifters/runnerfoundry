package daemon

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

func TestProductionRuntimeReportsRecoveryAndErrorCategories(t *testing.T) {
	originalLogging, originalState := newDaemonLoggingManager, openDaemonState
	t.Cleanup(func() { newDaemonLoggingManager, openDaemonState = originalLogging, originalState })

	destination := &memoryLogDestination{}
	newDaemonLoggingManager = func(config.LogConfig, logDestinationFactory) (*loggingManager, error) {
		return newLoggingManager(config.LogConfig{}, singleLogDestinationFactory{destination: destination})
	}
	openDaemonState = func(context.Context, config.Config) (*productionState, error) {
		return &productionState{member: "member-1"}, nil
	}

	runtime, err := openProductionRuntime(context.Background(), config.Config{})
	if err != nil {
		t.Fatalf("open production runtime: %v", err)
	}
	runtime.report(nil)
	runtime.report(context.Canceled)
	runtime.report(context.DeadlineExceeded)
	runtime.report(errors.New("operation failed"))

	output := destination.String()
	for _, expected := range []string{
		"outcome=recovered",
		"error_category=canceled",
		"error_category=deadline",
		"error_category=operation",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("runtime log %q does not contain %q", output, expected)
		}
	}
}

func TestPreparedLoggingCommitClosesReplacedDestination(t *testing.T) {
	t.Parallel()

	initial := &memoryLogDestination{}
	replacement := &memoryLogDestination{}
	manager := memoryLoggingManager(t, initial, replacement)
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Commit(); err == nil {
		t.Fatal("inactive logging change committed")
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Commit(); err != nil {
		t.Fatalf("commit logging change: %v", err)
	}
	if initial.closes != 1 || replacement.closes != 0 {
		t.Fatalf("destination closes = initial %d, replacement %d", initial.closes, replacement.closes)
	}
	if err = prepared.Commit(); err == nil {
		t.Fatal("completed logging change committed twice")
	}
}

func TestPreparedLoggingRollbackRestoresPreviousDestination(t *testing.T) {
	t.Parallel()

	initial := &memoryLogDestination{}
	replacement := &memoryLogDestination{}
	manager := memoryLoggingManager(t, initial, replacement)
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Rollback(); err != nil {
		t.Fatalf("rollback inactive change: %v", err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Rollback(); err != nil {
		t.Fatalf("rollback active change: %v", err)
	}
	if replacement.closes != 1 || initial.closes != 0 {
		t.Fatalf("destination closes = initial %d, replacement %d", initial.closes, replacement.closes)
	}
	if err = prepared.Rollback(); err != nil {
		t.Fatalf("rollback completed change: %v", err)
	}
}

func TestPreparedLoggingReportsCommitAndRollbackDestinationFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("destination close failed")
	manager := memoryLoggingManager(t, &memoryLogDestination{closeErr: wantErr}, &memoryLogDestination{})
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil || prepared.Activate() != nil {
		t.Fatalf("prepare commit failure fixture: %v", err)
	}
	if err = prepared.Commit(); !errors.Is(err, wantErr) {
		t.Fatalf("commit error = %v, want %v", err, wantErr)
	}

	manager = memoryLoggingManager(t, &memoryLogDestination{}, &memoryLogDestination{closeErr: wantErr})
	prepared, err = manager.Prepare(config.LogConfig{})
	if err != nil || prepared.Activate() != nil {
		t.Fatalf("prepare rollback failure fixture: %v", err)
	}
	if err = prepared.Rollback(); !errors.Is(err, wantErr) {
		t.Fatalf("rollback error = %v, want %v", err, wantErr)
	}
}

func TestPreparedLoggingCloseReportsActivatedDestinationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("candidate close failed")
	manager := memoryLoggingManager(t, &memoryLogDestination{}, &memoryLogDestination{closeErr: wantErr})
	prepared, err := manager.Prepare(config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Activate(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("close activated logging change = %v, want %v", err, wantErr)
	}
}

func TestReplaceActiveGenerationReportsHandoffFailures(t *testing.T) {
	t.Parallel()

	deactivateErr := errors.New("deactivate failed")
	replacementCloseErr := errors.New("replacement close failed")
	previous := &lifecycleGenerationFake{deactivateErr: deactivateErr}
	replacement := &lifecycleGenerationFake{closeErr: replacementCloseErr}
	handoff := &generationHandoff{active: &managedGeneration{generation: previous}}
	err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: replacement})
	if !errors.Is(err, deactivateErr) || !errors.Is(err, replacementCloseErr) || replacement.closes != 1 {
		t.Fatalf("failed deactivation handoff = %v, replacement closes %d", err, replacement.closes)
	}

	activateErr := errors.New("replacement activation failed")
	reactivateErr := errors.New("previous reactivation failed")
	previous = &lifecycleGenerationFake{activateErr: reactivateErr}
	replacement = &lifecycleGenerationFake{activateErr: activateErr}
	handoff = &generationHandoff{active: &managedGeneration{generation: previous}}
	err = replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: replacement})
	if !errors.Is(err, activateErr) || !errors.Is(err, reactivateErr) || previous.activations != 1 {
		t.Fatalf("failed activation handoff = %v, previous activations %d", err, previous.activations)
	}
}

func TestAffectedRunnerPoolsReportsInvalidProviderConfigurations(t *testing.T) {
	t.Parallel()
	invalid := config.Config{RunnerPools: []config.RunnerPoolConfig{{Name: "invalid"}}}
	valid := config.Config{RunnerPools: []config.RunnerPoolConfig{{Name: "docker", Docker: &config.DockerConfig{}}}}
	if _, err := affectedRunnerPools(valid, invalid); err == nil {
		t.Fatal("invalid replacement provider was accepted")
	}
	if _, err := affectedRunnerPools(invalid, valid); err == nil {
		t.Fatal("invalid active provider was accepted")
	}
	if err := terminateReplacedGeneration(context.Background(), &generationHandoff{policy: TerminateActiveRunners},
		&managedGeneration{configured: invalid}, &managedGeneration{configured: valid}); err == nil {
		t.Fatal("termination accepted invalid active provider")
	}
}

func TestUnifiedEngineStopsPoolWorkWhenInventoryRefreshFails(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("inventory unavailable")
	events := make([]string, 0)
	engine := unifiedEngineFixture(&events)
	engine.inventory = &runnerInventoryRefreshFake{err: wantErr}

	err := engine.Cycle(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("cycle error = %v, want %v", err, wantErr)
	}
	if occurrences(events, "reconcile:linux") != 0 || occurrences(events, "request:linux") != 0 {
		t.Fatalf("pool work continued after inventory failure: %#v", events)
	}
}

func TestGatedCycleEngineReportsDeactivationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("gate close failed")
	engine := newGatedCycleEngine(&lifecycleCycleEngineFake{}, &generationLeadershipFake{closeErr: wantErr})
	if err := engine.Deactivate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("deactivate gated engine = %v, want %v", err, wantErr)
	}
}

func TestMaintenanceGenerationReportsActivationCycleAndDeactivationFailures(t *testing.T) {
	t.Parallel()

	cycleErr := errors.New("first cycle failed")
	deactivateErr := errors.New("deactivate failed")
	engine := &lifecycleCycleEngineFake{cycleErr: cycleErr, deactivateErr: deactivateErr}
	generation := newMaintenanceGeneration(engine, &manualCycleTicker{ticks: make(chan time.Time)}, nil, nil, nil)
	if err := generation.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := generation.Activate(context.Background()); !errors.Is(err, cycleErr) || !errors.Is(err, deactivateErr) {
		t.Fatalf("activation error = %v, want cycle and deactivation failures", err)
	}

	engine.cycleErr = nil
	if err := generation.Activate(context.Background()); err != nil {
		t.Fatalf("activate retry: %v", err)
	}
	if err := generation.Deactivate(context.Background()); !errors.Is(err, deactivateErr) {
		t.Fatalf("deactivation error = %v, want %v", err, deactivateErr)
	}
}

func TestMaintenanceGenerationDeduplicatesFailuresAndReportsRecovery(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("cycle failed")
	reports := make([]error, 0)
	generation := newMaintenanceGeneration(
		&lifecycleCycleEngineFake{},
		&manualCycleTicker{ticks: make(chan time.Time)},
		func(err error) { reports = append(reports, err) },
		nil,
		nil,
	)
	generation.reportCycle(wantErr)
	generation.reportCycle(wantErr)
	generation.reportCycle(nil)
	if len(reports) != 2 || !errors.Is(reports[0], wantErr) || reports[1] != nil {
		t.Fatalf("cycle reports = %#v, want one failure and one recovery", reports)
	}
}

func TestMaintenanceGenerationRetriesOnlyFailedClosers(t *testing.T) {
	t.Parallel()

	failing := &failOnceRuntimeCloser{err: errors.New("close failed")}
	successful := &failOnceRuntimeCloser{}
	generation := newMaintenanceGeneration(
		&lifecycleCycleEngineFake{},
		&manualCycleTicker{ticks: make(chan time.Time)},
		nil,
		[]runtimeCloser{failing, successful},
		nil,
	)
	if err := generation.Close(context.Background()); !errors.Is(err, failing.err) {
		t.Fatalf("first close = %v, want %v", err, failing.err)
	}
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if failing.calls != 2 || successful.calls != 1 {
		t.Fatalf("closer calls = failing %d, successful %d", failing.calls, successful.calls)
	}
}

func TestLoggingGenerationDelegatesDeactivation(t *testing.T) {
	t.Parallel()

	generation := &lifecycleGenerationFake{}
	wrapper := &loggingGeneration{generation: generation, prepared: &preparedLogFake{events: &[]string{}}}
	if err := wrapper.Deactivate(context.Background()); err != nil || generation.deactivations != 1 {
		t.Fatalf("deactivate logging generation = %v, calls %d", err, generation.deactivations)
	}
}

func TestBuildProductionProvidersReportsDockerHostnameFailures(t *testing.T) {
	originalReadHostname := productionReadHostname
	t.Cleanup(func() { productionReadHostname = originalReadHostname })

	configured := config.Config{RunnerPools: []config.RunnerPoolConfig{{
		Name: "linux",
		Docker: &config.DockerConfig{
			ComposeFile: "compose.yml",
			Service:     "runner",
		},
	}}}
	wantErr := errors.New("hostname unavailable")
	productionReadHostname = func() (string, error) { return "", wantErr }
	if _, _, err := buildProductionProviders(context.Background(), configured); !errors.Is(err, wantErr) {
		t.Fatalf("hostname read error = %v, want %v", err, wantErr)
	}
	productionReadHostname = func() (string, error) { return " \n", nil }
	if _, _, err := buildProductionProviders(context.Background(), configured); err == nil {
		t.Fatal("empty Docker hostname was accepted")
	}
}

type singleLogDestinationFactory struct {
	destination io.WriteCloser
}

func (factory singleLogDestinationFactory) Open(config.LogConfig) (io.WriteCloser, error) {
	return factory.destination, nil
}

type lifecycleGenerationFake struct {
	activateErr   error
	deactivateErr error
	closeErr      error
	activations   int
	deactivations int
	closes        int
}

func (*lifecycleGenerationFake) Prepare(context.Context) error { return nil }
func (fake *lifecycleGenerationFake) Activate(context.Context) error {
	fake.activations++
	return fake.activateErr
}
func (fake *lifecycleGenerationFake) Deactivate(context.Context) error {
	fake.deactivations++
	return fake.deactivateErr
}
func (fake *lifecycleGenerationFake) Close(context.Context) error {
	fake.closes++
	return fake.closeErr
}
func (*lifecycleGenerationFake) Finalizers([]core.RunnerPool) []runtimeFinalizer { return nil }

type runnerInventoryRefreshFake struct {
	err error
}

func (*runnerInventoryRefreshFake) EndpointID() execution.InventoryEndpointID { return "inventory" }
func (fake *runnerInventoryRefreshFake) Refresh(context.Context) error        { return fake.err }
func (*runnerInventoryRefreshFake) Inspect(context.Context) ([]execution.RunnerInstance, error) {
	return nil, nil
}
func (*runnerInventoryRefreshFake) InspectEndpoint(context.Context, execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	return nil, nil
}
func (*runnerInventoryRefreshFake) KnowsEndpoint(execution.InventoryEndpointID) bool { return true }
func (*runnerInventoryRefreshFake) Destroy(context.Context, execution.RunnerInstance) error {
	return nil
}

type generationLeadershipFake struct {
	closeErr error
}

func (*generationLeadershipFake) IsLeader(context.Context) (bool, error) { return true, nil }
func (*generationLeadershipFake) Activate() error                        { return nil }
func (fake *generationLeadershipFake) Close(context.Context) error       { return fake.closeErr }

type lifecycleCycleEngineFake struct {
	cycleErr      error
	deactivateErr error
}

func (*lifecycleCycleEngineFake) Activate() error { return nil }
func (fake *lifecycleCycleEngineFake) Deactivate(context.Context) error {
	return fake.deactivateErr
}
func (fake *lifecycleCycleEngineFake) Cycle(context.Context) error { return fake.cycleErr }
func (*lifecycleCycleEngineFake) Close(context.Context) error      { return nil }

type failOnceRuntimeCloser struct {
	err   error
	calls int
}

func (closer *failOnceRuntimeCloser) Close(context.Context) error {
	closer.calls++
	if closer.calls == 1 {
		return closer.err
	}
	return nil
}
