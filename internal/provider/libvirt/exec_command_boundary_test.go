package libvirt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestExecCommandRunnerBuildsVirshCommandsInMemory(t *testing.T) {
	recording := &recordingVirshExecution{output: []byte("result\n")}
	runner := recording.runner()

	output, err := runner.ReadConnectionURI(context.Background(), "qemu:///system")
	if err != nil || strings.TrimSpace(output) != "result" {
		t.Fatalf("read connection URI: output=%q error=%v", output, err)
	}
	assertVirshArguments(t, recording.arguments, "-c", "qemu:///system", "uri")

	if _, err = runner.ListDomains(context.Background(), "qemu:///system"); err != nil {
		t.Fatal(err)
	}
	assertVirshArguments(t, recording.arguments, "-c", "qemu:///system", "list", "--all", "--name")

	if _, err = runner.ReadDomainXML(context.Background(), "qemu:///system", "runner"); err != nil {
		t.Fatal(err)
	}
	assertVirshArguments(t, recording.arguments, "-c", "qemu:///system", "dumpxml", "runner")

	if _, err = runner.ReadDomainState(context.Background(), "qemu:///system", "runner"); err != nil {
		t.Fatal(err)
	}
	assertVirshArguments(t, recording.arguments, "-c", "qemu:///system", "domstate", "runner")

	if err = runner.DestroyDomain(context.Background(), "qemu:///system", "runner"); err != nil {
		t.Fatal(err)
	}
	assertVirshArguments(t, recording.arguments, "-c", "qemu:///system", "destroy", "runner")
}

func TestExecCommandRunnerSuppliesDomainXMLOnStandardInput(t *testing.T) {
	recording := &recordingVirshExecution{}
	if err := recording.runner().CreateDomain(context.Background(), "qemu:///system", "<domain/>"); err != nil {
		t.Fatal(err)
	}
	assertVirshArguments(t, recording.arguments, "-c", "qemu:///system", "create", "/dev/stdin")
	if recording.standardInput != "<domain/>" {
		t.Fatalf("virsh standard input = %q", recording.standardInput)
	}
}

func TestExecCommandRunnerReportsOperationFailure(t *testing.T) {
	recording := &recordingVirshExecution{err: errors.New("failed")}
	if _, err := recording.runner().ReadConnectionURI(context.Background(), "qemu:///system"); err == nil || !strings.Contains(err.Error(), "virsh uri failed") {
		t.Fatalf("virsh failure = %v", err)
	}
}

func TestNewExecCommandRunnerInitializesCommandBoundaries(t *testing.T) {
	runner := NewExecCommandRunner()
	if runner.newCommand == nil || runner.combinedOutput == nil {
		t.Fatal("production command dependencies are incomplete")
	}
}

type recordingVirshExecution struct {
	arguments     []string
	standardInput string
	output        []byte
	err           error
}

func (recording *recordingVirshExecution) runner() ExecCommandRunner {
	return ExecCommandRunner{
		newCommand: func(_ context.Context, name string, arguments ...string) *exec.Cmd {
			recording.arguments = append([]string{name}, arguments...)
			return &exec.Cmd{}
		},
		combinedOutput: func(command *exec.Cmd) ([]byte, error) {
			if command.Stdin != nil {
				contents, err := io.ReadAll(command.Stdin)
				if err != nil {
					return nil, fmt.Errorf("read recorded standard input: %w", err)
				}
				recording.standardInput = string(contents)
			}
			return recording.output, recording.err
		},
	}
}

func assertVirshArguments(t *testing.T, actual []string, expected ...string) {
	t.Helper()
	if !slices.Equal(actual, append([]string{"virsh"}, expected...)) {
		t.Fatalf("virsh arguments = %#v, want %#v", actual, append([]string{"virsh"}, expected...))
	}
}
