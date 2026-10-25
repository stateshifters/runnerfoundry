package orchard

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func testProvider(t *testing.T, api *fakeAPI, guest GuestConnector) *Provider {
	t.Helper()
	provider, err := NewProvider(api, guest, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(`{"image":"base"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}
func ownedVM(pool string) v1.VM {
	return v1.VM{Meta: v1.Meta{Name: versionPrefix + pool + "-" + string(runnerName)}}
}

func TestInventoryRequiresPositiveConvergedRunningEvidence(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     v1.VMStatus
		generation uint64
		observed   uint64
		conditions []v1.Condition
		expected   execution.RunnerState
	}{
		{name: "running", status: v1.VMStatusRunning, generation: 2, observed: 2, conditions: []v1.Condition{{Type: v1.ConditionTypeRunning, State: v1.ConditionStateTrue}}, expected: execution.RunnerStateRunning},
		{name: "stopped", status: v1.VMStatusRunning, conditions: []v1.Condition{{Type: v1.ConditionTypeRunning, State: v1.ConditionStateFalse}}, expected: execution.RunnerStateCompleted},
		{name: "failed", status: v1.VMStatusFailed, expected: execution.RunnerStateCompleted},
		{name: "stale", status: v1.VMStatusRunning, generation: 2, observed: 1, conditions: []v1.Condition{{Type: v1.ConditionTypeRunning, State: v1.ConditionStateTrue}}, expected: execution.RunnerStateUnknown},
		{name: "pending", status: v1.VMStatusPending, expected: execution.RunnerStateUnknown},
		{name: "missing running evidence", status: v1.VMStatusRunning, expected: execution.RunnerStateUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertOrchardObservation(t, test.status, test.generation, test.observed, test.conditions, test.expected)
		})
	}
}

func assertOrchardObservation(t *testing.T, status v1.VMStatus, generation uint64, observed uint64, conditions []v1.Condition, expected execution.RunnerState) {
	t.Helper()
	vm := ownedVM("cG9vbA")
	vm.Status = status
	vm.Generation = generation
	vm.ObservedGeneration = observed
	vm.Conditions = conditions
	provider := testProvider(t, &fakeAPI{records: []v1.VM{{Meta: v1.Meta{Name: "unrelated"}}, vm, ownedVM("b3RoZXI")}}, nil)
	instances, err := provider.Inspect(context.Background())
	if err != nil || len(instances) != 2 || instances[0].State != expected || instances[1].RunnerPool != "other" {
		t.Fatalf("instances=%+v error=%v", instances, err)
	}
}

func TestInventoryFailsClosedAndRefreshDoesNotRetainStaleEvidence(t *testing.T) {
	for _, records := range [][]v1.VM{nil, {{}}, {{Meta: v1.Meta{Name: ownershipPrefix + "broken"}}}, {ownedVM("cG9vbA"), ownedVM("cG9vbA")}, {{Meta: v1.Meta{Name: versionPrefix + "***-" + string(runnerName)}}}, {ownedVM("")}, {ownedVM(strings.Repeat("cG9vbA", 40))}} {
		api := &fakeAPI{records: []v1.VM{ownedVM("cG9vbA")}}
		provider := testProvider(t, api, nil)
		if err := provider.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		api.records = records
		if err := provider.Refresh(context.Background()); err == nil {
			t.Fatalf("accepted %+v", records)
		}
		if instances, err := provider.Inspect(context.Background()); err == nil || len(instances) != 0 {
			t.Fatalf("stale evidence=%+v error=%v", instances, err)
		}
	}
}

func TestInventoryRejectsInitialUncertaintyAndForeignEndpoints(t *testing.T) {
	api := &fakeAPI{listError: errors.New("upstream")}
	provider := testProvider(t, api, nil)
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("uncertain inventory accepted")
	}
	if instances, err := provider.Inspect(context.Background()); err == nil || len(instances) != 0 {
		t.Fatal("initial uncertain snapshot accepted")
	}
	if _, err := provider.InspectEndpoint(context.Background(), "foreign"); err == nil {
		t.Fatal("foreign endpoint accepted")
	}
}

func TestDestructionRejectsForeignIdentityAndAcceptsOwnedAsynchronousRemoval(t *testing.T) {
	api := &fakeAPI{}
	provider := testProvider(t, api, nil)
	owned := execution.RunnerInstance{ID: ownedVM("cG9vbA").Name, RunnerPool: "pool", RunnerName: runnerName, EndpointID: provider.EndpointID()}
	for _, change := range []func(*execution.RunnerInstance){func(instance *execution.RunnerInstance) { instance.ID = "foreign" }, func(instance *execution.RunnerInstance) { instance.ID = ownershipPrefix + "broken" }, func(instance *execution.RunnerInstance) { instance.EndpointID = "foreign" }, func(instance *execution.RunnerInstance) { instance.RunnerPool = "other" }, func(instance *execution.RunnerInstance) { instance.RunnerName = core.RunnerName("foreign") }} {
		instance := owned
		change(&instance)
		if err := provider.Destroy(context.Background(), instance); err == nil {
			t.Fatalf("accepted %+v", instance)
		}
	}
	if len(api.deleted) != 0 {
		t.Fatal("foreign deletion reached controller")
	}
	if err := provider.Destroy(context.Background(), owned); err != nil || len(api.deleted) != 1 || api.lists != 0 {
		t.Fatalf("accepted asynchronous deletion=%v", err)
	}
	api.deleteError = errors.New("uncertain")
	if err := provider.Destroy(context.Background(), owned); err == nil {
		t.Fatal("uncertain deletion accepted")
	}
}
