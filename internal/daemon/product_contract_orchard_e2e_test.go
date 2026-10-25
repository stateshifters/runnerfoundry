//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	orchardprovider "github.com/stateshifters/runnerfoundry/internal/provider/orchard"
)

func TestProductContractOrchardRetriesAfterCleanupAndRecoversTerminalCleanup(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	fixture := newOrchardCompositionFixture(t)
	establishProductionCompositionSessions(t, fixture.tickers, fixture.host)
	processing, retried := provisionOrchardWithRetry(t, fixture.runtime, fixture.tickers, fixture.host, fixture.controller)
	observeOrchardAcceptanceAndTerminal(t, fixture.runtime, fixture.tickers, fixture.host, processing.RunnerName)
	fixture.reopen(t)
	fixture.assertTerminalCleanup(t, retried)
}

type orchardCompositionFixture struct {
	configured config.Config
	host       *productionCompositionGitHub
	controller *orchardLifecycleController
	runtime    *productionRuntime
	generation *runtimeGeneration
	tickers    productionCompositionTickers
}

func newOrchardCompositionFixture(t *testing.T) *orchardCompositionFixture {
	t.Helper()
	configured := loadProductionCompositionConfig(t)
	configured.State.Path = filepath.Join(t.TempDir(), "orchard-state.db")
	configured.RunnerPools[0].Docker = nil
	configured.RunnerPools[0].Orchard = &config.OrchardConfig{ControllerURL: "https://orchard.example", ServiceAccount: "runnerfoundry", TokenFile: "/orchard-token", VMSpecFile: "/orchard-spec"}
	fixture := &orchardCompositionFixture{configured: configured, host: newProductionCompositionGitHub(), controller: newOrchardLifecycleController()}
	fixture.open(t)
	t.Cleanup(func() {
		if fixture.runtime != nil {
			closeProductionComposition(context.Background(), t, fixture.runtime, fixture.generation)
		}
		fixture.host.assertExpected(t)
		if t.Failed() {
			t.Logf("Orchard lifecycle logs:\n%s", fixture.tickers.logs.String())
		}
	})
	return fixture
}

func (fixture *orchardCompositionFixture) open(t *testing.T) {
	t.Helper()
	configure := func(dependencies *productionDependencies) {
		originalReadFile := dependencies.readFile
		dependencies.readFile = func(path string) ([]byte, error) {
			switch path {
			case "/orchard-token":
				return []byte("token"), nil
			case "/orchard-spec":
				return []byte(`{"image":"macos-base"}`), nil
			default:
				return originalReadFile(path)
			}
		}
		dependencies.newOrchardProvider = func(configured orchardprovider.Config) (execution.ExecutionProvider, error) {
			return orchardprovider.NewProvider(fixture.controller, fixture.controller, configured)
		}
	}
	fixture.runtime, fixture.generation, fixture.tickers = openProductionComposition(t, fixture.configured, fixture.host, newProductionCompositionDockerCommands(), newProductionCompositionLibvirtCommands(), configure)
}

func (fixture *orchardCompositionFixture) reopen(t *testing.T) {
	t.Helper()
	closeProductionComposition(context.Background(), t, fixture.runtime, fixture.generation)
	fixture.runtime = nil
	fixture.open(t)
}

func (fixture *orchardCompositionFixture) assertTerminalCleanup(t *testing.T, vmName string) {
	t.Helper()
	if removed := awaitProductionCompositionValue(t, "terminal Orchard cleanup after reconstruction", fixture.controller.removals); removed != vmName {
		t.Fatalf("terminal Orchard removal = %q, want %q", removed, vmName)
	}
	fixture.tickers.complete(t, fixture.tickers.execution)
	assertNoProductionCompositionProcessing(t, fixture.runtime, "docker-pool")
	fixture.controller.assertEmpty(t)
}

func provisionOrchardWithRetry(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	controller *orchardLifecycleController,
) (core.ProcessingJob, string) {
	t.Helper()
	host.queue("29", 901, time.Date(2020, time.August, 16, 10, 0, 0, 0, time.UTC))
	tickers.awaitPublishedPolls(t, "docker-pool")
	tickers.complete(t, tickers.source)
	awaitProductionCompositionValue(t, "Orchard demand acknowledgement", host.acknowledged)
	tickers.complete(t, tickers.source)
	first := awaitProductionCompositionValue(t, "first Orchard bootstrap", controller.deliveries)
	job := assertProductionCompositionProcessing(t, runtime, "docker-pool").Job.ID
	close(controller.releaseFailure)
	removed := awaitProductionCompositionValue(t, "failed Orchard cleanup", controller.removals)
	if removed != first {
		t.Fatalf("failed Orchard removal = %q, want %q", removed, first)
	}
	tickers.complete(t, tickers.execution)
	retried := awaitProductionCompositionValue(t, "retried Orchard bootstrap", controller.deliveries)
	processing := assertProductionCompositionProcessing(t, runtime, "docker-pool")
	if processing.Job.ID != job || retried == first {
		t.Fatalf("Orchard retry = job %q vm %q; first job %q vm %q", processing.Job.ID, retried, job, first)
	}
	controller.assertBootstrap(t)
	return processing, retried
}

func observeOrchardAcceptanceAndTerminal(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	runnerName core.RunnerName,
) {
	t.Helper()
	host.queueStarted("29", 1901, 901, string(runnerName))
	tickers.awaitPublishedPolls(t, "docker-pool")
	tickers.complete(t, tickers.source)
	awaitProductionCompositionValue(t, "Orchard acceptance acknowledgement", host.acknowledged)
	tickers.complete(t, tickers.source)
	assertProductionCompositionObservation(t, runtime, "docker-pool", productionCompositionObservation{runnerStarted: true})
	host.queueCompleted("29", 2901, 901, string(runnerName))
	tickers.awaitPublishedPolls(t, "docker-pool")
	tickers.complete(t, tickers.source)
	awaitProductionCompositionValue(t, "Orchard terminal acknowledgement", host.acknowledged)
	tickers.complete(t, tickers.source)
	assertProductionCompositionObservation(t, runtime, "docker-pool", productionCompositionObservation{runnerStarted: true, sourceTerminal: true})
}

type orchardLifecycleController struct {
	mutex          sync.Mutex
	machines       map[string]v1.VM
	created        []string
	deleted        []string
	bootstrap      []string
	releaseFailure chan struct{}
	deliveries     chan string
	removals       chan string
}

func newOrchardLifecycleController() *orchardLifecycleController {
	return &orchardLifecycleController{
		machines: map[string]v1.VM{
			"macos-base": {
				Meta:    v1.Meta{Name: "macos-base"},
				Status:  v1.VMStatusRunning,
				VMState: v1.VMState{Conditions: []v1.Condition{{Type: v1.ConditionTypeRunning, State: v1.ConditionStateFalse}}},
			},
		},
		releaseFailure: make(chan struct{}),
		deliveries:     make(chan string, 8),
		removals:       make(chan string, 8),
	}
}

func (controller *orchardLifecycleController) Create(_ context.Context, vm *v1.VM) error {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	if vm.Name == "" || vm.Image != "macos-base" || vm.StartupScript != nil {
		return errors.New("unexpected Orchard VM creation specification")
	}
	if _, exists := controller.machines[vm.Name]; exists {
		return errors.New("duplicate Orchard VM creation")
	}
	machine := *vm
	machine.Generation = 1
	machine.ObservedGeneration = 1
	machine.Status = v1.VMStatusRunning
	machine.Conditions = []v1.Condition{{Type: v1.ConditionTypeRunning, State: v1.ConditionStateTrue}}
	controller.machines[vm.Name] = machine
	controller.created = append(controller.created, vm.Name)
	return nil
}

func (controller *orchardLifecycleController) List(context.Context) ([]v1.VM, error) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	machines := make([]v1.VM, 0, len(controller.machines))
	for _, machine := range controller.machines {
		machines = append(machines, machine)
	}
	return machines, nil
}

func (controller *orchardLifecycleController) Delete(_ context.Context, name string) error {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	controller.deleted = append(controller.deleted, name)
	delete(controller.machines, name)
	controller.removals <- name
	return nil
}

func (controller *orchardLifecycleController) Connect(_ context.Context, name string) (orchardprovider.Guest, error) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	if _, exists := controller.machines[name]; !exists {
		return orchardprovider.Guest{}, errors.New("connect to absent Orchard VM")
	}
	guest := &orchardLifecycleGuest{controller: controller, name: name}
	return orchardprovider.Guest{Exec: guest.Exec, Close: guest.Close}, nil
}

func (controller *orchardLifecycleController) deliver(ctx context.Context, name string, bootstrap []byte) error {
	controller.mutex.Lock()
	controller.bootstrap = append(controller.bootstrap, string(bootstrap))
	first := len(controller.bootstrap) == 1
	controller.mutex.Unlock()
	controller.deliveries <- name
	if first {
		select {
		case <-controller.releaseFailure:
			return errors.New("scripted guest delivery failure")
		case <-ctx.Done():
			return fmt.Errorf("scripted guest delivery canceled: %w", ctx.Err())
		}
	}
	return nil
}

func (controller *orchardLifecycleController) assertBootstrap(t *testing.T) {
	t.Helper()
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	if len(controller.bootstrap) != 2 || controller.bootstrap[0] != "production-composition-bootstrap" || controller.bootstrap[1] != controller.bootstrap[0] {
		t.Fatalf("Orchard bootstrap transport = %q", controller.bootstrap)
	}
}

func (controller *orchardLifecycleController) assertEmpty(t *testing.T) {
	t.Helper()
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	base, exists := controller.machines["macos-base"]
	if len(controller.machines) != 1 || !exists || !v1.ConditionIsFalse(base.Conditions, v1.ConditionTypeRunning) {
		t.Fatalf("Orchard cleanup left machines %+v", controller.machines)
	}
	if len(controller.created) != 2 || !slices.Equal(controller.created, controller.deleted) {
		t.Fatalf("Orchard owned cleanup: created %q, deleted %q", controller.created, controller.deleted)
	}
}

type orchardLifecycleGuest struct {
	controller *orchardLifecycleController
	name       string
}

func (guest *orchardLifecycleGuest) Exec(ctx context.Context) (orchardprovider.ExecStream, error) {
	stream := &orchardLifecycleExecStream{guest: guest, complete: func(content []byte) error { return guest.controller.deliver(ctx, guest.name, content) }}
	return orchardprovider.ExecStream{Send: stream.Send, CloseSend: stream.CloseSend, Recv: stream.Recv}, nil
}
func (*orchardLifecycleGuest) Close() error { return nil }

type orchardLifecycleExecStream struct {
	complete  func([]byte) error
	guest     *orchardLifecycleGuest
	command   *guestagent.ExecRequest_Command
	bootstrap []byte
	closed    bool
	received  bool
}

func (stream *orchardLifecycleExecStream) Send(request *guestagent.ExecRequest) error {
	if stream.closed {
		return errors.New("guest request after input closed")
	}
	switch content := request.GetType().(type) {
	case *guestagent.ExecRequest_Command_:
		if stream.command != nil || !orchardLifecycleFIFOCommand(content.Command) {
			return errors.New("unexpected Orchard guest command")
		}
		stream.command = content.Command
	case *guestagent.ExecRequest_StandardInput:
		if stream.command == nil || stream.command.Name != "/bin/sh" {
			return errors.New("bootstrap input without FIFO delivery command")
		}
		stream.bootstrap = append(stream.bootstrap, content.StandardInput.GetData()...)
	default:
		return errors.New("unexpected Orchard guest request")
	}
	return nil
}

func orchardLifecycleFIFOCommand(command *guestagent.ExecRequest_Command) bool {
	if command == nil || command.Tty {
		return false
	}
	if command.Name == "/bin/test" {
		return slices.Equal(command.Args, []string{"-p", "/var/run/runnerfoundry/bootstrap"}) && !command.Interactive
	}
	return command.Name == "/bin/sh" && slices.Equal(command.Args, []string{"-c", "test -p /var/run/runnerfoundry/bootstrap && exec /bin/cat > /var/run/runnerfoundry/bootstrap"}) && command.Interactive
}

func (stream *orchardLifecycleExecStream) CloseSend() error { stream.closed = true; return nil }

func (stream *orchardLifecycleExecStream) Recv() (*guestagent.ExecResponse, error) {
	if stream.received {
		return nil, io.EOF
	}
	stream.received = true
	if stream.command == nil || !stream.closed {
		return nil, errors.New("guest result before command and input completion")
	}
	if stream.command.Name == "/bin/sh" {
		if err := stream.complete(stream.bootstrap); err != nil {
			return nil, err
		}
	}
	return &guestagent.ExecResponse{Type: &guestagent.ExecResponse_Exit_{Exit: &guestagent.ExecResponse_Exit{Code: 0}}}, nil
}
