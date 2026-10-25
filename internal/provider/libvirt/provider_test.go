package libvirt

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestStartCreatesTransientDomainFromRenderedStdin(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := providerWithTemplate(commandRunner, `<domain type="kvm"><name>template</name></domain>`)
	provider.config.ConnectionURI = "qemu:///system"

	err := provider.Start(context.Background(), execution.StartRequest{
		RunnerPool: "vm-set",
		RunnerName: "vm-set-1",
		Bootstrap:  core.NewBootstrap([]byte("secret")),
	})
	if err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	expectedCommands := []Command{
		{Args: []string{"-c", "qemu:///system", "create", "/dev/stdin"}, Stdin: commandRunner.commands[0].Stdin},
	}
	if !reflect.DeepEqual(commandRunner.commands, expectedCommands) {
		t.Fatalf("unexpected commands\nwant %#v\ngot  %#v", expectedCommands, commandRunner.commands)
	}
	if commandRunner.commands[0].Stdin == "" {
		t.Fatal("expected rendered domain XML on stdin")
	}
	if !containsAll(commandRunner.commands[0].Stdin, []string{"<name>vm-set-1</name>", "runnerfoundry:runner-pool", "vm-set", "runnerfoundry.jit=secret"}) {
		t.Fatalf("expected rendered runner metadata and SMBIOS JIT, got %s", commandRunner.commands[0].Stdin)
	}
	if containsAll(commandRunner.commands[0].Stdin, []string{"runnerfoundry.jit-env"}) {
		t.Fatalf("jit config must not be stored in metadata: %s", commandRunner.commands[0].Stdin)
	}
}

func TestListRecognizesLegacyScaleSetMetadata(t *testing.T) {
	instances, _ := listOwnedDomainFixture(t)
	expected := []execution.RunnerInstance{
		{ID: "vm-set-1", RunnerName: "vm-set-1", RunnerPool: "vm-set", State: execution.RunnerStateRunning},
		{ID: "vm-set-2", RunnerName: "vm-set-2", RunnerPool: "vm-set", State: execution.RunnerStateCompleted},
	}
	if !reflect.DeepEqual(instances, expected) {
		t.Fatalf("unexpected instances\nwant %#v\ngot  %#v", expected, instances)
	}
}

func TestListRecognizesRunnerPoolMetadata(t *testing.T) {
	commandRunner := &recordingCommandRunner{outputs: []string{
		"runner-1\n",
		`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:runner-pool>linux</runnerfoundry:runner-pool><runnerfoundry:name>runner-1</runnerfoundry:name></runnerfoundry:runner></metadata></domain>`,
		"running\n",
	}}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []execution.RunnerInstance{{
		ID:         "runner-1",
		RunnerName: "runner-1",
		RunnerPool: "linux",
		State:      execution.RunnerStateRunning,
	}}
	if !reflect.DeepEqual(instances, want) {
		t.Fatalf("List() instances = %#v, want %#v", instances, want)
	}
}

func TestInspectCombinesNamespacedAndRawLegacyOwnershipFields(t *testing.T) {
	commandRunner := &recordingCommandRunner{outputs: []string{
		"mixed-pool\nmixed-runner\nconflicting-pool\n",
		`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:name>namespaced-name</runnerfoundry:name></runnerfoundry:runner><runnerfoundry.scale-set>legacy-pool</runnerfoundry.scale-set></metadata></domain>`,
		"running\n",
		`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:runner-pool>current-pool</runnerfoundry:runner-pool></runnerfoundry:runner><runnerfoundry.runner>legacy-name</runnerfoundry.runner></metadata></domain>`,
		"running\n",
		`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:scale-set>legacy-pool</runnerfoundry:scale-set></runnerfoundry:runner><runnerfoundry.runner-pool>current-pool</runnerfoundry.runner-pool></metadata></domain>`,
		"running\n",
	}}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	instances, err := provider.Inspect(context.Background())
	want := []execution.RunnerInstance{
		{ID: "mixed-pool", RunnerName: "namespaced-name", RunnerPool: "legacy-pool", State: execution.RunnerStateRunning},
		{ID: "mixed-runner", RunnerName: "legacy-name", RunnerPool: "current-pool", State: execution.RunnerStateRunning},
		{ID: "conflicting-pool", RunnerName: "conflicting-pool", RunnerPool: "current-pool", State: execution.RunnerStateRunning},
	}
	if err != nil || !reflect.DeepEqual(instances, want) {
		t.Fatalf("mixed ownership inventory = (%#v, %v), want %#v", instances, err, want)
	}
}

func TestListInspectsOnlyOwnedDomainStates(t *testing.T) {
	_, commands := listOwnedDomainFixture(t)
	expected := []Command{
		{Args: []string{"-c", "qemu:///system", "list", "--all", "--name"}},
		{Args: []string{"-c", "qemu:///system", "dumpxml", "vm-set-1"}},
		{Args: []string{"-c", "qemu:///system", "domstate", "vm-set-1"}},
		{Args: []string{"-c", "qemu:///system", "dumpxml", "vm-set-2"}},
		{Args: []string{"-c", "qemu:///system", "domstate", "vm-set-2"}},
		{Args: []string{"-c", "qemu:///system", "dumpxml", "other"}},
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("unexpected commands\nwant %#v\ngot  %#v", expected, commands)
	}
}

func listOwnedDomainFixture(t *testing.T) ([]execution.RunnerInstance, []Command) {
	t.Helper()
	runner := &recordingCommandRunner{
		outputs: []string{
			"vm-set-1\nvm-set-2\nother\n",
			`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:scale-set>vm-set</runnerfoundry:scale-set><runnerfoundry:name>vm-set-1</runnerfoundry:name></runnerfoundry:runner></metadata></domain>`,
			"running\n",
			`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:scale-set>vm-set</runnerfoundry:scale-set><runnerfoundry:name>vm-set-2</runnerfoundry:name></runnerfoundry:runner></metadata></domain>`,
			"shut off\n",
			`<domain><metadata><example>unowned</example></metadata></domain>`,
		},
	}
	provider := NewProvider(runner,
		Config{ConnectionURI: "qemu:///system"}, nil, "")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	return instances, runner.commands
}

func TestListClassifiesUnknownDomainState(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		outputs: []string{
			"vm-set-1\n",
			`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:scale-set>vm-set</runnerfoundry:scale-set><runnerfoundry:name>vm-set-1</runnerfoundry:name></runnerfoundry:runner></metadata></domain>`,
			"paused\n",
		},
	}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 1 || instances[0].State != execution.RunnerStateUnknown {
		t.Fatalf("expected unknown state, got %#v", instances)
	}
}

func TestListUsesDomainNameWhenRunnerMetadataIsMissing(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		outputs: []string{
			"vm-set-1\n",
			`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:scale-set>vm-set</runnerfoundry:scale-set></runnerfoundry:runner></metadata></domain>`,
			"running\n",
		},
	}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 1 || instances[0].RunnerName != "vm-set-1" {
		t.Fatalf("expected domain name fallback, got %#v", instances)
	}
}

func TestListIgnoresDomainsWithoutRunnerFoundryOwnership(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		outputs: []string{
			"unowned\nowned\n",
			`<domain><metadata><example>unowned</example></metadata></domain>`,
			`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:scale-set>vm-set</runnerfoundry:scale-set><runnerfoundry:name>owned</runnerfoundry:name></runnerfoundry:runner></metadata></domain>`,
			"running\n",
		},
	}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []execution.RunnerInstance{{
		ID:         "owned",
		RunnerName: "owned",
		RunnerPool: "vm-set",
		State:      execution.RunnerStateRunning,
	}}
	if !reflect.DeepEqual(instances, want) {
		t.Fatalf("List() instances = %#v, want %#v", instances, want)
	}
}

func TestDestroyForceStopsRunningTransientDomainWithoutRemovingStorage(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	if err := provider.Destroy(context.Background(), execution.RunnerInstance{ID: "vm-set-1", RunnerName: "vm-set-1", State: execution.RunnerStateRunning}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
	expected := []string{"-c", "qemu:///system", "destroy", "vm-set-1"}
	if !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("unexpected command\nwant %#v\ngot  %#v", expected, commandRunner.commands[0].Args)
	}
}

func TestDestroyIgnoresCompletedTransientDomains(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")

	if err := provider.Destroy(context.Background(), execution.RunnerInstance{ID: "vm-set-1", RunnerName: "vm-set-1", State: execution.RunnerStateCompleted}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
	if len(commandRunner.commands) != 0 {
		t.Fatalf("expected no virsh command for completed transient domain, got %#v", commandRunner.commands)
	}
}

func TestDestroyForceStopsPausedOrSuspendedDomain(t *testing.T) {
	for _, state := range []string{"paused", "pmsuspended"} {
		t.Run(state, func(t *testing.T) {
			commandRunner := &recordingCommandRunner{}
			provider := NewProvider(commandRunner, Config{ConnectionURI: "qemu:///system"}, nil, "")
			if err := provider.Destroy(context.Background(), execution.RunnerInstance{
				ID: "vm-set-1", RunnerName: "vm-set-1", State: libvirtState(state),
			}); err != nil {
				t.Fatal(err)
			}
			if len(commandRunner.commands) != 1 || commandRunner.commands[0].Args[2] != "destroy" {
				t.Fatalf("commands = %#v", commandRunner.commands)
			}
		})
	}
}

func TestDestroyUnknownReportsCommandFailure(t *testing.T) {
	provider := NewProvider(failingCommandRunner{}, Config{}, nil, "")
	if err := provider.Destroy(context.Background(), execution.RunnerInstance{State: execution.RunnerStateUnknown}); err == nil {
		t.Fatal("expected unknown destroy failure")
	}
}

func TestValidateParsesTemplateAndChecksConnection(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := providerWithTemplate(commandRunner, `<domain type="kvm"><name>template</name></domain>`)
	provider.config.ConnectionURI = "qemu:///system"

	if err := provider.Validate(context.Background()); err != nil {
		t.Fatalf("expected validation success, got %v", err)
	}
	expected := []string{"-c", "qemu:///system", "uri"}
	if !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("unexpected command\nwant %#v\ngot  %#v", expected, commandRunner.commands[0].Args)
	}
}

func TestStartRequiresTemplate(t *testing.T) {
	provider := NewProvider(&recordingCommandRunner{}, Config{}, nil, "")
	if err := provider.Start(context.Background(), execution.StartRequest{}); err == nil {
		t.Fatal("expected missing template error")
	}
}

func TestNewProviderOwnsConfigurationAndTemplate(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := NewProvider(commandRunner, Config{DomainXMLTemplate: "domain.xml"}, []byte("<domain/>"), "member-a")
	if provider.commandRunner != commandRunner || string(provider.domainTemplate) != "<domain/>" ||
		provider.config.DomainXMLTemplate != "domain.xml" || provider.memberID != "member-a" {
		t.Fatalf("production provider = %#v", provider)
	}
}

func TestStartRejectsInvalidTemplate(t *testing.T) {
	provider := providerWithTemplate(&recordingCommandRunner{}, `<domain><name>broken</domain>`)
	if err := provider.Start(context.Background(), execution.StartRequest{}); err == nil {
		t.Fatal("expected invalid template error")
	}
}

func TestStartReportsCreateCommandFailure(t *testing.T) {
	provider := providerWithTemplate(failingCommandRunner{}, `<domain type="kvm"><name>template</name></domain>`)
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerName: "runner"}); err == nil {
		t.Fatal("expected create error")
	}
}

func TestListReportsCommandFailure(t *testing.T) {
	provider := NewProvider(failingCommandRunner{}, Config{}, nil, "")
	if _, err := provider.Inspect(context.Background()); err == nil {
		t.Fatal("expected list command error")
	}
}

func TestListReportsDomainInspectionFailure(t *testing.T) {
	commandRunner := &recordingCommandRunner{outputs: []string{"runner\n"}, failOnCall: 2}
	provider := NewProvider(commandRunner, Config{}, nil, "")
	if _, err := provider.Inspect(context.Background()); err == nil {
		t.Fatal("expected domain inspection error")
	}
}

func TestListReportsDomainStateFailure(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		outputs:    []string{"runner\n", `<domain><metadata><runnerfoundry.scale-set>set</runnerfoundry.scale-set></metadata></domain>`},
		failOnCall: 3,
	}
	provider := NewProvider(commandRunner, Config{}, nil, "")
	if _, err := provider.Inspect(context.Background()); err == nil {
		t.Fatal("expected domain state error")
	}
}

func TestDestroyReportsCommandFailure(t *testing.T) {
	provider := NewProvider(failingCommandRunner{}, Config{}, nil, "")
	if err := provider.Destroy(context.Background(), execution.RunnerInstance{State: execution.RunnerStateRunning}); err == nil {
		t.Fatal("expected destroy command error")
	}
}

func TestValidateRejectsInvalidTemplate(t *testing.T) {
	provider := providerWithTemplate(&recordingCommandRunner{}, `<domain><name>broken</domain>`)
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("expected validation template error")
	}
}

func TestValidateReportsConnectionFailure(t *testing.T) {
	provider := providerWithTemplate(failingCommandRunner{}, `<domain type="kvm"><name>template</name></domain>`)
	if err := provider.Validate(context.Background()); err == nil {
		t.Fatal("expected validation connection error")
	}
}

func TestLegacyMetadataValueHandlesMissingTags(t *testing.T) {
	if legacyMetadataValue("<domain></domain>", "missing") != "" {
		t.Fatal("expected missing metadata to be empty")
	}
	if legacyMetadataValue("<domain><missing>value</domain>", "missing") != "" {
		t.Fatal("expected missing closing tag to be empty")
	}
}

func providerWithTemplate(commandRunner CommandRunner, contents string) Provider {
	return NewProvider(commandRunner, Config{DomainXMLTemplate: "domain.xml"}, []byte(contents), "")
}

func containsAll(text string, needles []string) bool {
	for _, needle := range needles {
		if !contains(text, needle) {
			return false
		}
	}
	return true
}

func contains(text string, needle string) bool {
	return strings.Contains(text, needle)
}

type recordingCommandRunner struct {
	commands   []Command
	outputs    []string
	failOnCall int
}

type Command struct {
	Args  []string
	Stdin string
}

func (runner *recordingCommandRunner) record(command Command) (string, error) {
	runner.commands = append(runner.commands, command)
	if runner.failOnCall == len(runner.commands) {
		return "", errors.New("command failed")
	}
	if len(runner.outputs) == 0 {
		return "", nil
	}
	output := runner.outputs[0]
	runner.outputs = runner.outputs[1:]
	return output, nil
}

func (runner *recordingCommandRunner) CreateDomain(_ context.Context, connectionURI string, domainXML string) error {
	_, err := runner.record(Command{Args: []string{"-c", connectionURI, "create", "/dev/stdin"}, Stdin: domainXML})
	return err
}

func (runner *recordingCommandRunner) ListDomains(_ context.Context, connectionURI string) (string, error) {
	return runner.record(Command{Args: []string{"-c", connectionURI, "list", "--all", "--name"}})
}

func (runner *recordingCommandRunner) ReadDomainXML(_ context.Context, connectionURI string, domainName string) (string, error) {
	return runner.record(Command{Args: []string{"-c", connectionURI, "dumpxml", domainName}})
}

func (runner *recordingCommandRunner) ReadDomainState(_ context.Context, connectionURI string, domainName string) (string, error) {
	return runner.record(Command{Args: []string{"-c", connectionURI, "domstate", domainName}})
}

func (runner *recordingCommandRunner) DestroyDomain(_ context.Context, connectionURI string, domainName string) error {
	_, err := runner.record(Command{Args: []string{"-c", connectionURI, "destroy", domainName}})
	return err
}

func (runner *recordingCommandRunner) ReadConnectionURI(_ context.Context, connectionURI string) (string, error) {
	return runner.record(Command{Args: []string{"-c", connectionURI, "uri"}})
}

type failingCommandRunner struct{}

func (runner failingCommandRunner) CreateDomain(context.Context, string, string) error {
	return errors.New("command failed")
}
func (runner failingCommandRunner) ListDomains(context.Context, string) (string, error) {
	return "", errors.New("command failed")
}
func (runner failingCommandRunner) ReadDomainXML(context.Context, string, string) (string, error) {
	return "", errors.New("command failed")
}
func (runner failingCommandRunner) ReadDomainState(context.Context, string, string) (string, error) {
	return "", errors.New("command failed")
}
func (runner failingCommandRunner) DestroyDomain(context.Context, string, string) error {
	return errors.New("command failed")
}
func (runner failingCommandRunner) ReadConnectionURI(context.Context, string) (string, error) {
	return "", errors.New("command failed")
}
