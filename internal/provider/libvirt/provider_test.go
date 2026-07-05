package libvirt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStartDefinesRenderedDomainFromStdinAndStartsIt(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := Provider{
		CommandRunner: commandRunner,
		Config: Config{
			ConnectionURI:     "qemu:///system",
			DomainXMLTemplate: writeTemplate(t, `<domain type="kvm"><name>template</name></domain>`),
		},
	}

	err := provider.Start(context.Background(), StartRequest{
		ScaleSetName: "vm-set",
		RunnerName:   "vm-set-1",
		JITConfig:    "secret",
	})
	if err != nil {
		t.Fatalf("expected start success, got %v", err)
	}
	expectedCommands := []Command{
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "define", "/dev/stdin"}, Stdin: commandRunner.commands[0].Stdin},
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "start", "vm-set-1"}},
	}
	if !reflect.DeepEqual(commandRunner.commands, expectedCommands) {
		t.Fatalf("unexpected commands\nwant %#v\ngot  %#v", expectedCommands, commandRunner.commands)
	}
	if commandRunner.commands[0].Stdin == "" {
		t.Fatal("expected rendered domain XML on stdin")
	}
	if !containsAll(commandRunner.commands[0].Stdin, []string{"<name>vm-set-1</name>", "github-scaler.scale-set", "vm-set"}) {
		t.Fatalf("expected rendered scaler metadata, got %s", commandRunner.commands[0].Stdin)
	}
	if containsAll(commandRunner.commands[0].Stdin, []string{"secret"}) {
		t.Fatalf("jit config must not be embedded directly in XML: %s", commandRunner.commands[0].Stdin)
	}
}

func TestListRebuildsOwnedDomainState(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		outputs: []string{
			"vm-set-1\nvm-set-2\nother\n",
			`<domain><metadata><github-scaler.scale-set>vm-set</github-scaler.scale-set><github-scaler.runner>vm-set-1</github-scaler.runner></metadata></domain>`,
			"running\n",
			`<domain><metadata><github-scaler.scale-set>vm-set</github-scaler.scale-set><github-scaler.runner>vm-set-2</github-scaler.runner></metadata></domain>`,
			"shut off\n",
			`<domain><metadata><github-scaler.scale-set>other</github-scaler.scale-set><github-scaler.runner>other</github-scaler.runner></metadata></domain>`,
		},
	}
	provider := Provider{
		CommandRunner: commandRunner,
		Config:        Config{ConnectionURI: "qemu:///system"},
	}

	instances, err := provider.List(context.Background(), "vm-set")
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	expectedInstances := []RunnerInstance{
		{ID: "vm-set-1", RunnerName: "vm-set-1", State: RunnerStateRunning},
		{ID: "vm-set-2", RunnerName: "vm-set-2", State: RunnerStateCompleted},
	}
	if !reflect.DeepEqual(instances, expectedInstances) {
		t.Fatalf("unexpected instances\nwant %#v\ngot  %#v", expectedInstances, instances)
	}
	expectedCommands := []Command{
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "list", "--all", "--name"}},
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "dumpxml", "vm-set-1"}},
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "domstate", "vm-set-1"}},
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "dumpxml", "vm-set-2"}},
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "domstate", "vm-set-2"}},
		{Name: "virsh", Args: []string{"-c", "qemu:///system", "dumpxml", "other"}},
	}
	if !reflect.DeepEqual(commandRunner.commands, expectedCommands) {
		t.Fatalf("unexpected commands\nwant %#v\ngot  %#v", expectedCommands, commandRunner.commands)
	}
}

func TestListUsesDomainNameWhenRunnerMetadataIsMissing(t *testing.T) {
	commandRunner := &recordingCommandRunner{
		outputs: []string{
			"vm-set-1\n",
			`<domain><metadata><github-scaler.scale-set>vm-set</github-scaler.scale-set></metadata></domain>`,
			"running\n",
		},
	}
	provider := Provider{CommandRunner: commandRunner, Config: Config{ConnectionURI: "qemu:///system"}}

	instances, err := provider.List(context.Background(), "vm-set")
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(instances) != 1 || instances[0].RunnerName != "vm-set-1" {
		t.Fatalf("expected domain name fallback, got %#v", instances)
	}
}

func TestDestroyUndefinesCompletedDomain(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := Provider{CommandRunner: commandRunner, Config: Config{ConnectionURI: "qemu:///system"}}

	if err := provider.Destroy(context.Background(), RunnerInstance{ID: "vm-set-1", RunnerName: "vm-set-1"}); err != nil {
		t.Fatalf("expected destroy success, got %v", err)
	}
	expected := []string{"-c", "qemu:///system", "undefine", "vm-set-1"}
	if !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("unexpected command\nwant %#v\ngot  %#v", expected, commandRunner.commands[0].Args)
	}
}

func TestValidateParsesTemplateAndChecksConnection(t *testing.T) {
	commandRunner := &recordingCommandRunner{}
	provider := Provider{
		CommandRunner: commandRunner,
		Config: Config{
			ConnectionURI:     "qemu:///system",
			DomainXMLTemplate: writeTemplate(t, `<domain type="kvm"><name>template</name></domain>`),
		},
	}

	if err := provider.Validate(context.Background()); err != nil {
		t.Fatalf("expected validation success, got %v", err)
	}
	expected := []string{"-c", "qemu:///system", "uri"}
	if !reflect.DeepEqual(commandRunner.commands[0].Args, expected) {
		t.Fatalf("unexpected command\nwant %#v\ngot  %#v", expected, commandRunner.commands[0].Args)
	}
}

func TestProviderSurfacesFailures(t *testing.T) {
	validTemplate := writeTemplate(t, `<domain type="kvm"><name>template</name></domain>`)
	invalidTemplate := writeTemplate(t, `<domain><name>broken</domain>`)
	tests := map[string]func(t *testing.T){
		"start missing template": func(t *testing.T) {
			provider := Provider{CommandRunner: &recordingCommandRunner{}, Config: Config{DomainXMLTemplate: filepath.Join(t.TempDir(), "missing")}}
			if err := provider.Start(context.Background(), StartRequest{}); err == nil {
				t.Fatal("expected missing template error")
			}
		},
		"start invalid template": func(t *testing.T) {
			provider := Provider{CommandRunner: &recordingCommandRunner{}, Config: Config{DomainXMLTemplate: invalidTemplate}}
			if err := provider.Start(context.Background(), StartRequest{}); err == nil {
				t.Fatal("expected invalid template error")
			}
		},
		"start define command": func(t *testing.T) {
			provider := Provider{CommandRunner: failingCommandRunner{}, Config: Config{DomainXMLTemplate: validTemplate}}
			if err := provider.Start(context.Background(), StartRequest{RunnerName: "runner"}); err == nil {
				t.Fatal("expected define error")
			}
		},
		"start command": func(t *testing.T) {
			commandRunner := &recordingCommandRunner{failOnCall: 2}
			provider := Provider{CommandRunner: commandRunner, Config: Config{DomainXMLTemplate: validTemplate}}
			if err := provider.Start(context.Background(), StartRequest{RunnerName: "runner"}); err == nil {
				t.Fatal("expected start command error")
			}
		},
		"list command": func(t *testing.T) {
			provider := Provider{CommandRunner: failingCommandRunner{}}
			if _, err := provider.List(context.Background(), "set"); err == nil {
				t.Fatal("expected list command error")
			}
		},
		"dump command": func(t *testing.T) {
			commandRunner := &recordingCommandRunner{outputs: []string{"runner\n"}, failOnCall: 2}
			provider := Provider{CommandRunner: commandRunner}
			if _, err := provider.List(context.Background(), "set"); err == nil {
				t.Fatal("expected dump command error")
			}
		},
		"state command": func(t *testing.T) {
			commandRunner := &recordingCommandRunner{
				outputs:    []string{"runner\n", `<domain><metadata><github-scaler.scale-set>set</github-scaler.scale-set></metadata></domain>`},
				failOnCall: 3,
			}
			provider := Provider{CommandRunner: commandRunner}
			if _, err := provider.List(context.Background(), "set"); err == nil {
				t.Fatal("expected state command error")
			}
		},
		"destroy command": func(t *testing.T) {
			provider := Provider{CommandRunner: failingCommandRunner{}}
			if err := provider.Destroy(context.Background(), RunnerInstance{}); err == nil {
				t.Fatal("expected destroy command error")
			}
		},
		"validate template": func(t *testing.T) {
			provider := Provider{CommandRunner: &recordingCommandRunner{}, Config: Config{DomainXMLTemplate: invalidTemplate}}
			if err := provider.Validate(context.Background()); err == nil {
				t.Fatal("expected validation template error")
			}
		},
		"validate connection": func(t *testing.T) {
			provider := Provider{CommandRunner: failingCommandRunner{}, Config: Config{DomainXMLTemplate: validTemplate}}
			if err := provider.Validate(context.Background()); err == nil {
				t.Fatal("expected validation connection error")
			}
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, testCase)
	}
}

func TestExecCommandRunner(t *testing.T) {
	output, err := (ExecCommandRunner{}).Run(context.Background(), Command{Name: "go", Args: []string{"version"}})
	if err != nil {
		t.Fatalf("expected exec success, got %v", err)
	}
	if output == "" {
		t.Fatal("expected command output")
	}
	output, err = (ExecCommandRunner{}).Run(context.Background(), Command{Name: "go", Args: []string{"env", "GOVERSION"}, Stdin: "ignored"})
	if err != nil {
		t.Fatalf("expected exec with stdin success, got %v", err)
	}
	if output == "" {
		t.Fatal("expected command output")
	}
	if _, err := (ExecCommandRunner{}).Run(context.Background(), Command{Name: "definitely-missing-github-scaler-command"}); err == nil {
		t.Fatal("expected exec failure")
	}
}

func TestMetadataValueHandlesMissingTags(t *testing.T) {
	if metadataValue("<domain></domain>", "missing") != "" {
		t.Fatal("expected missing metadata to be empty")
	}
	if metadataValue("<domain><missing>value</domain>", "missing") != "" {
		t.Fatal("expected missing closing tag to be empty")
	}
}

func writeTemplate(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "domain.xml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
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

func (runner *recordingCommandRunner) Run(ctx context.Context, command Command) (string, error) {
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

type failingCommandRunner struct{}

func (runner failingCommandRunner) Run(ctx context.Context, command Command) (string, error) {
	return "", errors.New("command failed")
}
