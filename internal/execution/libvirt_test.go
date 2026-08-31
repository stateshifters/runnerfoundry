package execution

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	providerlibvirt "github.com/stateshifters/runnerfoundry/internal/provider/libvirt"
)

func TestLibvirtStartTransportsOpaqueBootstrapInGuestConfiguration(t *testing.T) {
	transport := &recordingLibvirtTransport{}
	provider := NewLibvirtProvider(
		transport,
		providerlibvirt.Config{ConnectionURI: "qemu:///system"},
		[]byte(`<domain type="kvm"><name>template</name></domain>`),
		"member-a",
	)

	err := provider.Start(context.Background(), StartRequest{
		RunnerPool: "linux-vm",
		RunnerName: "linux-vm-1",
		Bootstrap:  core.NewBootstrap([]byte(`opaque<&bootstrap`)),
	})
	if err != nil {
		t.Fatalf("start libvirt runner: %v", err)
	}
	if transport.connectionURI != "qemu:///system" {
		t.Fatalf("connection URI = %q, want qemu:///system", transport.connectionURI)
	}
	for _, expected := range []string{
		"<name>linux-vm-1</name>",
		"linux-vm",
		"runnerfoundry.jit=opaque&lt;&amp;bootstrap",
	} {
		if !strings.Contains(transport.domainXML, expected) {
			t.Fatalf("domain XML does not contain %q: %s", expected, transport.domainXML)
		}
	}
}

func TestLibvirtInspectReturnsOwnedRunnerState(t *testing.T) {
	transport := &recordingLibvirtTransport{
		domains: "linux-vm-1\nlinux-vm-2\n",
		domainXMLByName: map[string]string{
			"linux-vm-1": libvirtOwnedDomainXML("linux-vm", "linux-vm-1"),
			"linux-vm-2": libvirtOwnedDomainXML("linux-vm", "linux-vm-2"),
		},
		domainStateByName: map[string]string{
			"linux-vm-1": "running",
			"linux-vm-2": "shut off",
		},
	}
	provider := NewLibvirtProvider(transport, libvirtTestConfig(), []byte(`<domain><name>template</name></domain>`), "member-a")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect libvirt runners: %v", err)
	}
	expected := []RunnerInstance{
		{ID: "linux-vm-1", RunnerName: "linux-vm-1", RunnerPool: "linux-vm", State: RunnerStateRunning},
		{ID: "linux-vm-2", RunnerName: "linux-vm-2", RunnerPool: "linux-vm", State: RunnerStateCompleted},
	}
	if len(instances) != len(expected) {
		t.Fatalf("instances = %#v, want %#v", instances, expected)
	}
	for index := range expected {
		if instances[index] != expected[index] {
			t.Fatalf("instance %d = %#v, want %#v", index, instances[index], expected[index])
		}
	}
}

func TestLibvirtDestroyTranslatesRunnerIdentityAndState(t *testing.T) {
	transport := &recordingLibvirtTransport{}
	provider := NewLibvirtProvider(transport, libvirtTestConfig(), []byte(`<domain><name>template</name></domain>`), "member-a")

	err := provider.Destroy(context.Background(), RunnerInstance{
		ID:         "linux-vm-1",
		RunnerName: "linux-vm-1",
		State:      RunnerStateRunning,
	})
	if err != nil {
		t.Fatalf("destroy libvirt runner: %v", err)
	}
	if transport.destroyedDomain != "linux-vm-1" {
		t.Fatalf("destroyed domain = %q, want linux-vm-1", transport.destroyedDomain)
	}
}

func TestLibvirtValidateChecksTemplateAndConnection(t *testing.T) {
	transport := &recordingLibvirtTransport{}
	provider := NewLibvirtProvider(
		transport,
		libvirtTestConfig(),
		[]byte(`<domain type="kvm"><name>template</name></domain>`),
		"member-a",
	)

	if err := provider.Validate(context.Background()); err != nil {
		t.Fatalf("validate libvirt provider: %v", err)
	}
	if transport.validatedConnection != "qemu:///system" {
		t.Fatalf("validated connection = %q, want qemu:///system", transport.validatedConnection)
	}
}

func TestLibvirtValidatePreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("validate failed")
	provider := NewLibvirtProvider(
		&recordingLibvirtTransport{connectionErr: expectedErr},
		libvirtTestConfig(),
		[]byte(`<domain type="kvm"><name>template</name></domain>`),
		"member-a",
	)

	err := provider.Validate(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("validate error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestLibvirtInspectPreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("inspect failed")
	provider := NewLibvirtProvider(
		&recordingLibvirtTransport{listErr: expectedErr},
		libvirtTestConfig(),
		[]byte(`<domain><name>template</name></domain>`),
		"member-a",
	)

	_, err := provider.Inspect(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("inspect error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestLibvirtStartPreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("start failed")
	provider := NewLibvirtProvider(
		&recordingLibvirtTransport{createErr: expectedErr},
		libvirtTestConfig(),
		[]byte(`<domain><name>template</name></domain>`),
		"member-a",
	)

	err := provider.Start(context.Background(), StartRequest{})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("start error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestLibvirtDestroyPreservesTransportFailure(t *testing.T) {
	expectedErr := errors.New("destroy failed")
	provider := NewLibvirtProvider(
		&recordingLibvirtTransport{destroyErr: expectedErr},
		libvirtTestConfig(),
		[]byte(`<domain><name>template</name></domain>`),
		"member-a",
	)

	err := provider.Destroy(context.Background(), RunnerInstance{State: RunnerStateRunning})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("destroy error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestLibvirtEndpointOperationsAreExplicit(t *testing.T) {
	provider := NewLibvirtProvider(&recordingLibvirtTransport{}, libvirtTestConfig(), libvirtTemplate(), "member-b")
	endpointID := InventoryEndpointID("libvirt:qemu:///system")
	if provider.EndpointID() != endpointID || !provider.KnowsEndpoint(endpointID) || provider.KnowsEndpoint("unknown") {
		t.Fatalf("endpoint identity = %q", provider.EndpointID())
	}
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if _, err := provider.InspectEndpoint(context.Background(), endpointID); err != nil {
		t.Fatalf("inspect endpoint: %v", err)
	}
	if _, err := provider.InspectEndpoint(context.Background(), "unknown"); err == nil {
		t.Fatal("unknown endpoint accepted")
	}
}

func TestLibvirtEndpointInspectionRespectsLocalAndRemoteOwnership(t *testing.T) {
	local := NewLibvirtProvider(&recordingLibvirtTransport{}, libvirtTestConfig(), libvirtTemplate(), "member-b")
	if !local.CanInspectEndpointForMember(local.EndpointID(), "member-b") ||
		local.CanInspectEndpointForMember(local.EndpointID(), "member-a") ||
		local.CanInspectEndpointForMember("libvirt:unknown", "member-b") {
		t.Fatal("local libvirt endpoint must be inspectable only by its owning member")
	}
	remoteConfig := libvirtTestConfig()
	remoteConfig.ConnectionURI = "qemu+ssh://hypervisor.example/system"
	remote := NewLibvirtProvider(&recordingLibvirtTransport{}, remoteConfig, libvirtTemplate(), "member-b")
	if !remote.CanInspectEndpointForMember(remote.EndpointID(), "member-a") {
		t.Fatal("explicit remote libvirt endpoint must be inspectable by another member")
	}
	invalidConfig := libvirtTestConfig()
	invalidConfig.ConnectionURI = "qemu://host/%zz"
	invalid := NewLibvirtProvider(&recordingLibvirtTransport{}, invalidConfig, libvirtTemplate(), "member-b")
	if !invalid.CanInspectEndpointForMember(invalid.EndpointID(), "member-b") {
		t.Fatal("an unparseable libvirt URI must fail closed to its owning member")
	}
}

func TestLibvirtEndpointNormalizesConnectionURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want InventoryEndpointID
	}{
		{name: "scheme and host", uri: " QEMU+SSH://RUNNER.EXAMPLE/system ", want: "libvirt:qemu+ssh://runner.example/system"},
		{name: "unparseable URI remains stable", uri: " qemu://host/%zz ", want: "libvirt:qemu://host/%zz"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configured := libvirtTestConfig()
			configured.ConnectionURI = test.uri
			provider := NewLibvirtProvider(&recordingLibvirtTransport{}, configured, []byte("<domain/>"), "member-a")
			if got := provider.EndpointID(); got != test.want {
				t.Fatalf("endpoint ID = %q, want %q", got, test.want)
			}
		})
	}
}

type recordingLibvirtTransport struct {
	connectionURI       string
	domainXML           string
	createErr           error
	domains             string
	listErr             error
	listCalls           int
	domainXMLByName     map[string]string
	domainStateByName   map[string]string
	destroyedDomain     string
	destroyErr          error
	validatedConnection string
	connectionErr       error
}

func (transport *recordingLibvirtTransport) CreateDomain(
	_ context.Context,
	connectionURI string,
	domainXML string,
) error {
	transport.connectionURI = connectionURI
	transport.domainXML = domainXML
	return transport.createErr
}

func (transport *recordingLibvirtTransport) ListDomains(context.Context, string) (string, error) {
	transport.listCalls++
	return transport.domains, transport.listErr
}

func (transport *recordingLibvirtTransport) ReadDomainXML(
	_ context.Context,
	_ string,
	domainName string,
) (string, error) {
	return transport.domainXMLByName[domainName], nil
}

func (transport *recordingLibvirtTransport) ReadDomainState(
	_ context.Context,
	_ string,
	domainName string,
) (string, error) {
	return transport.domainStateByName[domainName], nil
}

func (transport *recordingLibvirtTransport) DestroyDomain(
	_ context.Context,
	_ string,
	domainName string,
) error {
	transport.destroyedDomain = domainName
	return transport.destroyErr
}

func (transport *recordingLibvirtTransport) ReadConnectionURI(
	_ context.Context,
	connectionURI string,
) (string, error) {
	transport.validatedConnection = connectionURI
	return "qemu:///system", transport.connectionErr
}

func libvirtTestConfig() providerlibvirt.Config {
	return providerlibvirt.Config{ConnectionURI: "qemu:///system"}
}

func libvirtOwnedDomainXML(runnerPool string, runnerName string) string {
	return `<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry">` +
		`<runnerfoundry:scale-set>` + runnerPool + `</runnerfoundry:scale-set>` +
		`<runnerfoundry:name>` + runnerName + `</runnerfoundry:name>` +
		`</runnerfoundry:runner></metadata></domain>`
}
