package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestPrepareCoordinatedLifecycleCoversLibvirtAndUnsupportedStore(t *testing.T) {
	logs := &bytes.Buffer{}
	configuration := coordinatedLifecycleConfig{
		Logger: newClusterLogSnapshotLogger(logs), RepositoryURL: "repo",
		ScaleSetConfig: config.ScaleSetConfig{Name: "linux", Libvirt: &config.LibvirtConfig{}},
		Coordinator:    scaler.NewDurableCoordinator("linux", 1, failingDaemonDemandStore{}),
	}
	prepareCoordinatedLifecycle(context.Background(), &configuration)
	if configuration.RunnerReapRetries == nil || !strings.Contains(logs.String(), "runner_lifecycle_configure") {
		t.Fatalf("prepared lifecycle retries=%#v logs=%q", configuration.RunnerReapRetries, logs.String())
	}
}

func TestStandaloneControlInitializesDurableCapacityCoordinator(t *testing.T) {
	store := newDaemonCapacityObligationStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	control := startCapacityObligationControl(t, coordinator, slog.Default())
	defer control.Stop()
	if _, err := coordinator.ObserveAssignedCapacity(context.Background(), "repo", 0, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if store.observation.ControllerEpoch != 1 {
		t.Fatalf("controller epoch = %d, want 1", store.observation.ControllerEpoch)
	}
}

func TestStandaloneControlReportsDurableCapacitySchedulingFailure(t *testing.T) {
	store := newDaemonCapacityObligationStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	logs := &bytes.Buffer{}
	control := startCapacityObligationControl(t, coordinator, slog.New(slog.NewTextHandler(logs, nil)))
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", MaxRunners: 1, Provider: failingStartProvider{}, JITConfig: fakeJITConfigGenerator{},
	}}})
	store.obligations = []scaler.QueuedDemand{{
		JobDemand: scaler.JobDemand{Repository: "repo"}, ObligationID: 1,
		Source: scaler.CapacityObligationAssignedCount, ControllerEpoch: 1,
	}}
	if err := coordinator.Schedule(context.Background()); err != nil {
		control.Stop()
		t.Fatal(err)
	}
	control.Stop()
	coordinator.WaitForScheduling()
	if !strings.Contains(logs.String(), "standalone capacity obligation failed") {
		t.Fatalf("scheduling failure log = %q", logs.String())
	}
}

func startCapacityObligationControl(
	t *testing.T,
	coordinator *scaler.Coordinator,
	logger *slog.Logger,
) *standaloneControl {
	t.Helper()
	active := testActiveConfiguration()
	active.Credentials.AppID = 1
	active.RepositoryState = newMemoryStandaloneState()
	active.Coordinators = map[string]*scaler.Coordinator{"linux": coordinator}
	active.Logger = logger
	control, err := startStandaloneControl(context.Background(), standaloneDaemon{}, &active, standaloneControlOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return control
}

func TestStartCoordinatedLifecycleReportsInitialCapacityPersistenceFailure(t *testing.T) {
	store := newDaemonCapacityObligationStore()
	store.observeErr = errors.New("observation unavailable")
	err := startCapacityObligationLifecycle(store)
	if err == nil || !strings.Contains(err.Error(), "persist initial assigned runner capacity") {
		t.Fatalf("error = %v", err)
	}
}

func TestStartCoordinatedLifecycleReportsInitialCapacityScheduleFailure(t *testing.T) {
	store := newDaemonCapacityObligationStore()
	store.orderedErr = errors.New("queue unavailable")
	err := startCapacityObligationLifecycle(store)
	if err == nil || !strings.Contains(err.Error(), "schedule initial runner capacity") {
		t.Fatalf("error = %v", err)
	}
}

func startCapacityObligationLifecycle(store *daemonCapacityObligationStore) error {
	messageClient := &fakeMessageClient{session: upstream.RunnerScaleSetSession{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}}
	_, err := startCoordinatedLifecycle(context.Background(), coordinatedLifecycleConfig{
		Coordinator:    scaler.NewDurableCoordinator("linux", 1, store),
		RepositoryURL:  "repo",
		ScaleSetConfig: testActiveConfiguration().Config.ScaleSets[0],
		MessageClient:  scaleSetMessageClient{Client: messageClient},
	})
	return err
}

func TestCoordinatedRepositoryWorkerStartsAssignedObligation(t *testing.T) {
	logs := &bytes.Buffer{}
	provider := &fakeScalerProvider{}
	worker := coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		Logger:     slog.New(slog.NewTextHandler(logs, nil)),
		Repository: "repo",
		ScaleSet:   "linux",
	}

	if err := worker.StartAssignedObligation(context.Background(), 73); err != nil {
		t.Fatal(err)
	}
	if len(provider.started) != 1 || provider.started[0].RunnerName != "linux-73" {
		t.Fatalf("started runners = %#v", provider.started)
	}
	if !strings.Contains(logs.String(), "obligationID=73") || !strings.Contains(logs.String(), "source=assigned_count") {
		t.Fatalf("assigned obligation log = %q", logs.String())
	}

	worker.Reconciler.ScaleSet.Provider = failingStartProvider{}
	if err := worker.StartAssignedObligation(context.Background(), 74); err == nil ||
		!strings.Contains(err.Error(), "start assigned repository runner") {
		t.Fatalf("error = %v", err)
	}
}

type daemonCapacityObligationStore struct {
	*scaler.MemoryDemandStore
	observation  scaler.AssignedCapacityObservation
	decision     scaler.AssignedObligationDecision
	observeErr   error
	orderedErr   error
	orderedCalls int
	obligations  []scaler.QueuedDemand
}

func newDaemonCapacityObligationStore() *daemonCapacityObligationStore {
	return &daemonCapacityObligationStore{MemoryDemandStore: scaler.NewMemoryDemandStore()}
}

func (store *daemonCapacityObligationStore) ObserveAssignedCapacity(
	_ context.Context,
	observation scaler.AssignedCapacityObservation,
	_ int,
) (scaler.AssignedObligationDecision, error) {
	store.observation = observation
	return store.decision, store.observeErr
}

func (store *daemonCapacityObligationStore) OrderedReady(
	context.Context,
	string,
	time.Time,
) ([]scaler.QueuedDemand, error) {
	store.orderedCalls++
	return store.obligations, store.orderedErr
}

func (*daemonCapacityObligationStore) RecordObligationFailure(context.Context, int64, time.Time) error {
	return nil
}

func (*daemonCapacityObligationStore) DeleteObligation(context.Context, int64) error { return nil }
