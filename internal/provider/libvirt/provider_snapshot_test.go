package libvirt

import (
	"context"
	"reflect"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/execution"
)

const snapshotDomainXML = `<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:runner-pool>pool</runnerfoundry:runner-pool></runnerfoundry:runner></metadata></domain>`

func TestInventorySnapshotChangesOnlyAfterRefresh(t *testing.T) {
	commands := &recordingCommandRunner{outputs: []string{"first", snapshotDomainXML, "running", "second", snapshotDomainXML, "shut off"}}
	provider := NewProvider(commands, Config{ConnectionURI: "qemu:///system"}, nil, "member")
	first := inspectLibvirtSnapshot(t, provider)
	first[0].ID = "mutated"
	cached := inspectLibvirtSnapshot(t, provider)
	if len(cached) != 1 || cached[0].ID != "first" || len(commands.commands) != 3 {
		t.Fatalf("snapshot=%+v calls=%+v", cached, commands.commands)
	}
	copyOfProvider := provider
	if err := copyOfProvider.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	refreshed := inspectLibvirtSnapshot(t, provider)
	if len(refreshed) != 1 || refreshed[0].ID != "second" || len(commands.commands) != 6 {
		t.Fatalf("refreshed=%+v calls=%+v", refreshed, commands.commands)
	}
}

func TestEndpointInspectionRemainsFreshWithoutReplacingCycleSnapshot(t *testing.T) {
	commands := &recordingCommandRunner{outputs: []string{"first", snapshotDomainXML, "running", "second", snapshotDomainXML, "shut off"}}
	provider := NewProvider(commands, Config{ConnectionURI: "qemu:///system"}, nil, "member")
	initial := inspectLibvirtSnapshot(t, provider)
	fresh, err := provider.InspectEndpoint(context.Background(), provider.EndpointID())
	if err != nil || len(fresh) != 1 || fresh[0].ID != "second" || fresh[0].EndpointID != provider.EndpointID() {
		t.Fatalf("endpoint=%+v error=%v", fresh, err)
	}
	if cached := inspectLibvirtSnapshot(t, provider); !reflect.DeepEqual(cached, initial) {
		t.Fatalf("endpoint inspection replaced snapshot=%+v", cached)
	}
}

func TestFailedRefreshInvalidatesCompleteLibvirtSnapshot(t *testing.T) {
	commands := &recordingCommandRunner{outputs: []string{"first", snapshotDomainXML, "running", "second\nthird", snapshotDomainXML, "running"}, failOnCall: 7}
	provider := NewProvider(commands, Config{ConnectionURI: "qemu:///system"}, nil, "member")
	inspectLibvirtSnapshot(t, provider)
	if err := provider.Refresh(context.Background()); err == nil {
		t.Fatal("partial observation accepted")
	}
	commands.failOnCall = 0
	commands.outputs = []string{"recovered", snapshotDomainXML, "running"}
	if instances, err := provider.Inspect(context.Background()); err == nil || len(instances) != 0 {
		t.Fatalf("uncertain snapshot=%+v error=%v", instances, err)
	}
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if instances := inspectLibvirtSnapshot(t, provider); len(instances) != 1 || instances[0].ID != "recovered" {
		t.Fatalf("recovered=%+v", instances)
	}
}

func TestRefreshPublishesEmptyLibvirtSnapshot(t *testing.T) {
	commands := &recordingCommandRunner{outputs: []string{"first", snapshotDomainXML, "running", ""}}
	provider := NewProvider(commands, Config{ConnectionURI: "qemu:///system"}, nil, "member")
	inspectLibvirtSnapshot(t, provider)
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if instances := inspectLibvirtSnapshot(t, provider); len(instances) != 0 {
		t.Fatalf("removed domains remain in snapshot=%+v", instances)
	}
}

func inspectLibvirtSnapshot(t *testing.T, provider Provider) []execution.RunnerInstance {
	t.Helper()
	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return instances
}
