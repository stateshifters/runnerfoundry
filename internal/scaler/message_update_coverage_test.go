package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidMessageUpdateRejectsInvalidDemandsAndIdentities(t *testing.T) {
	valid := validSQLiteMessageUpdate()
	invalid := []MessageUpdate{
		messageUpdateWithAvailable(valid, JobDemand{Repository: "repo", QueueTime: valid.ObservedAt}),
		messageUpdateWithAvailable(valid, JobDemand{Repository: "repo", RequestID: 1}),
		messageUpdateWithAvailable(valid, JobDemand{Repository: "other", RequestID: 1, QueueTime: valid.ObservedAt}),
		messageUpdateWithStarted(valid, RunnerMessageIdentity{}),
		messageUpdateWithCompleted(valid, RunnerMessageIdentity{}),
	}
	for index, update := range invalid {
		if ValidMessageUpdate(update) {
			t.Fatalf("invalid message update %d accepted", index)
		}
	}
}

func messageUpdateWithAvailable(update MessageUpdate, demand JobDemand) MessageUpdate {
	update.Available = []JobDemand{demand}
	return update
}

func messageUpdateWithStarted(update MessageUpdate, identity RunnerMessageIdentity) MessageUpdate {
	update.Started = []RunnerMessageIdentity{identity}
	return update
}

func messageUpdateWithCompleted(update MessageUpdate, identity RunnerMessageIdentity) MessageUpdate {
	update.Completed = []RunnerMessageIdentity{identity}
	return update
}

func TestMemoryMessageUpdateAppliesAllEffects(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	now := time.Unix(300, 0).UTC()
	seedMemoryMessageRunner(t, store, now)
	update := MessageUpdate{
		ScaleSet: "linux", Repository: "repo", ObservedAt: now,
		Available: []JobDemand{{Repository: "repo", RequestID: 2, QueueTime: now}},
		Started: []RunnerMessageIdentity{
			{RunnerIdentity: RunnerIdentity{RequestID: 1, RunnerName: "linux-1"}},
			{RunnerIdentity: RunnerIdentity{RequestID: 404}},
		},
		Completed: []RunnerMessageIdentity{{
			RunnerIdentity: RunnerIdentity{RequestID: 1, RunnerName: "linux-1"},
		}},
	}
	if _, err := store.ApplyMessageUpdate(ctx, update); err != nil {
		t.Fatal(err)
	}
	assertMemoryMessageEffects(t, store)
}

func TestMemoryMessageUpdatePropagatesStateFailures(t *testing.T) {
	failure := errors.New("failed")
	update := MessageUpdate{
		ScaleSet: "linux", Repository: "repo", ObservedAt: time.Unix(300, 0).UTC(),
		Started:   []RunnerMessageIdentity{{RunnerIdentity: RunnerIdentity{RequestID: 1}}},
		Completed: []RunnerMessageIdentity{{RunnerIdentity: RunnerIdentity{RequestID: 1}}},
	}
	tests := []struct {
		name  string
		state *faultingMemoryMessageState
	}{
		{name: "demand", state: &faultingMemoryMessageState{applyErr: failure}},
		{name: "started runner", state: &faultingMemoryMessageState{busyErr: failure}},
		{name: "runner lifecycle completion", state: &faultingMemoryMessageState{lifecycleErr: failure}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := applyMemoryMessageUpdate(context.Background(), test.state, update); !errors.Is(err, failure) {
				t.Fatalf("message update error = %v, want %v", err, failure)
			}
		})
	}
}

type faultingMemoryMessageState struct {
	applyErr     error
	busyErr      error
	lifecycleErr error
}

func (state *faultingMemoryMessageState) Apply(context.Context, string, []JobDemand, []DemandRemoval) error {
	return state.applyErr
}

func (state *faultingMemoryMessageState) MarkRunnerBusy(
	context.Context, string, string, RunnerIdentity, int64, time.Time,
) error {
	return state.busyErr
}

func (state *faultingMemoryMessageState) BindRunnerIdentity(
	context.Context, string, string, RunnerIdentity, int64, time.Time,
) error {
	return state.busyErr
}

func (state *faultingMemoryMessageState) CompleteRunnerLifecycleJob(
	context.Context, JobCompletion, time.Time,
) error {
	return state.lifecycleErr
}

func seedMemoryMessageRunner(t *testing.T, store *MemoryDemandStore, now time.Time) {
	t.Helper()
	if err := store.RegisterRunner(context.Background(), RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "linux-1", RequestID: 1, Phase: RunnerWaitingForJob, CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
}

func assertMemoryMessageEffects(t *testing.T, store *MemoryDemandStore) {
	t.Helper()
	ordered, err := store.Ordered(context.Background(), "linux")
	if err != nil || len(ordered) != 1 || ordered[0].RequestID != 2 {
		t.Fatalf("remaining demand = %#v, %v", ordered, err)
	}
	runners, err := store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(runners) != 1 || runners[0].Phase != RunnerReapPending {
		t.Fatalf("runner lifecycle = %#v, %v", runners, err)
	}
}

func TestCoordinatorMessageUpdatePaths(t *testing.T) {
	t.Run("unsupported", testCoordinatorMessageUpdateUnsupported)
	t.Run("capacity failure", testCoordinatorMessageUpdateCapacityFailure)
	t.Run("store failure", testCoordinatorMessageUpdateStoreFailure)
	t.Run("transient success", testCoordinatorMessageUpdateTransientSuccess)
	t.Run("durable success", testCoordinatorMessageUpdateDurableSuccess)
}

func testCoordinatorMessageUpdateUnsupported(t *testing.T) {
	coordinator := NewDurableCoordinator("linux", 1, &messageUnsupportedStore{DemandStore: NewMemoryDemandStore()})
	if _, err := coordinator.ApplyMessageUpdate(context.Background(), MessageUpdate{}); err == nil {
		t.Fatal("unsupported message update succeeded")
	}
}

func testCoordinatorMessageUpdateCapacityFailure(t *testing.T) {
	want := errors.New("inventory failed")
	store := &messageInventoryStore{MemoryDemandStore: NewMemoryDemandStore(), inventoryErr: want}
	coordinator := NewDurableCoordinator("linux", 1, store)
	if _, err := coordinator.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate()); !errors.Is(err, want) {
		t.Fatalf("capacity error = %v, want %v", err, want)
	}
}

func testCoordinatorMessageUpdateStoreFailure(t *testing.T) {
	want := errors.New("store failed")
	store := &messageFaultStore{MemoryDemandStore: NewMemoryDemandStore(), applyErr: want}
	coordinator := NewDurableCoordinator("linux", 1, store)
	if _, err := coordinator.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate()); !errors.Is(err, want) {
		t.Fatalf("store error = %v, want %v", err, want)
	}
}

func testCoordinatorMessageUpdateTransientSuccess(t *testing.T) {
	store := &messageFaultStore{MemoryDemandStore: NewMemoryDemandStore()}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.SetControllerIdentity(7, 3)
	result, err := coordinator.ApplyMessageUpdate(context.Background(), MessageUpdate{
		Repository: "repo", AssignedJobs: 2, ObservedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil || result.AssignedObligations.Create != 2 || coordinator.assigned["repo"] != 2 {
		t.Fatalf("transient result = %#v, assigned = %d, error = %v", result, coordinator.assigned["repo"], err)
	}
	if store.update.AppID != 7 || store.update.ControllerEpoch != 3 || store.update.ScaleSet != "linux" {
		t.Fatalf("coordinator metadata = %#v", store.update)
	}
}

func testCoordinatorMessageUpdateDurableSuccess(t *testing.T) {
	store := &messageDurableStore{messageFaultStore: messageFaultStore{MemoryDemandStore: NewMemoryDemandStore()}}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.SetControllerIdentity(7, 3)
	if _, err := coordinator.ApplyMessageUpdate(context.Background(), MessageUpdate{
		Repository: "repo", AssignedJobs: 2, ObservedAt: time.Unix(1, 0).UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, observed := coordinator.assigned["repo"]; observed {
		t.Fatal("durable message update also changed transient assigned state")
	}
}

type messageUnsupportedStore struct{ DemandStore }

type messageFaultStore struct {
	*MemoryDemandStore
	applyErr error
	update   MessageUpdate
}

func (store *messageFaultStore) ApplyMessageUpdate(_ context.Context, update MessageUpdate) (MessageUpdateResult, error) {
	store.update = update
	return MessageUpdateResult{AssignedObligations: AssignedObligationDecision{Create: update.AssignedJobs}}, store.applyErr
}

type messageInventoryStore struct {
	*MemoryDemandStore
	inventoryErr error
}

func (store *messageInventoryStore) RunnerLifecycles(context.Context, string, string) ([]RunnerLifecycle, error) {
	return nil, store.inventoryErr
}

type messageDurableStore struct{ messageFaultStore }

func (*messageDurableStore) ObserveAssignedCapacity(context.Context, AssignedCapacityObservation, int) (AssignedObligationDecision, error) {
	return AssignedObligationDecision{}, nil
}
func (*messageDurableStore) OrderedReady(context.Context, string, time.Time) ([]QueuedDemand, error) {
	return nil, nil
}
func (*messageDurableStore) RecordObligationFailure(context.Context, int64, time.Time) error {
	return nil
}
func (*messageDurableStore) DeleteObligation(context.Context, int64) error { return nil }

func TestCoordinatorSetControllerIdentity(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.SetControllerIdentity(11, 12)
	if coordinator.controllerAppID != 11 || coordinator.controllerEpoch != 12 {
		t.Fatalf("controller identity = %d/%d", coordinator.controllerAppID, coordinator.controllerEpoch)
	}
}

func TestCoordinatorMessageUpdateErrorsHaveContext(t *testing.T) {
	store := &messageFaultStore{MemoryDemandStore: NewMemoryDemandStore(), applyErr: errors.New("failed")}
	coordinator := NewDurableCoordinator("linux", 1, store)
	_, err := coordinator.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate())
	if err == nil || !strings.Contains(err.Error(), "persist scale-set message update") {
		t.Fatalf("message update error = %v", err)
	}
}
