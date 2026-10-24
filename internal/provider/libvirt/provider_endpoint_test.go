package libvirt

import (
	"context"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestProviderOwnsEndpointCompatibility(t *testing.T) {
	local := NewProvider(&recordingCommandRunner{}, Config{ConnectionURI: " QEMU:///SYSTEM "}, []byte("<domain/>"), "member-a")
	endpointID := execution.InventoryEndpointID("libvirt:qemu:///SYSTEM")
	if local.EndpointID() != endpointID || !local.KnowsEndpoint(endpointID) ||
		!local.CanInspectEndpointForMember(endpointID, "MEMBER-A") ||
		local.CanInspectEndpointForMember(endpointID, "member-b") ||
		local.CanInspectEndpointForMember("libvirt:qemu:///other", "member-a") {
		t.Fatalf("local endpoint identity = %q", local.EndpointID())
	}
	remote := NewProvider(&recordingCommandRunner{}, Config{ConnectionURI: "QEMU+SSH://HOST/system"}, []byte("<domain/>"), "member-a")
	if !remote.CanInspectEndpointForMember(remote.EndpointID(), "member-b") {
		t.Fatal("remote endpoint was restricted to its configuring member")
	}
	if _, err := local.InspectEndpoint(context.Background(), "libvirt:qemu:///other"); err == nil {
		t.Fatal("unknown endpoint was accepted")
	}
	invalid := NewProvider(&recordingCommandRunner{}, Config{ConnectionURI: "%"}, []byte("<domain/>"), "member-a")
	if invalid.EndpointID() != "libvirt:%" {
		t.Fatalf("invalid URI endpoint = %q", invalid.EndpointID())
	}
}

func TestProviderRefreshesAndInspectsKnownEndpoint(t *testing.T) {
	commandRunner := &recordingCommandRunner{outputs: []string{
		"runner-1\n",
		`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:runner-pool>linux</runnerfoundry:runner-pool></runnerfoundry:runner></metadata></domain>`,
		"running\n",
		"runner-1\n",
		`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:runner-pool>linux</runnerfoundry:runner-pool></runnerfoundry:runner></metadata></domain>`,
		"running\n",
	}}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, []byte("<domain/>"), "member-a")
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	instances, err := provider.InspectEndpoint(context.Background(), provider.EndpointID())
	if err != nil || len(instances) != 1 || instances[0].EndpointID != provider.EndpointID() {
		t.Fatalf("endpoint inventory = (%#v, %v)", instances, err)
	}
}

func TestProviderEndpointInspectionPreservesFailure(t *testing.T) {
	provider := NewProvider(failingCommandRunner{}, Config{ConnectionURI: "qemu:///system"}, []byte("<domain/>"), "member-a")
	if _, err := provider.InspectEndpoint(context.Background(), provider.EndpointID()); err == nil {
		t.Fatal("endpoint inspection failure was discarded")
	}
}
