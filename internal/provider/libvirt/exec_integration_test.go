//go:build integration

package libvirt

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMain(testSuite *testing.M) {
	if strings.EqualFold(filepath.Base(os.Args[0]), "virsh") || strings.EqualFold(filepath.Base(os.Args[0]), "virsh.exe") {
		runVirshShim()
		return
	}
	os.Exit(testSuite.Run())
}

func TestExecCommandRunnerInvokesVirshFromPath(t *testing.T) {
	shimDirectory := t.TempDir()
	writeVirshShim(t, shimDirectory)
	t.Setenv("PATH", shimDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))

	output, err := (ExecCommandRunner{}).ReadConnectionURI(context.Background(), "qemu:///system")
	if err != nil {
		t.Fatalf("run virsh shim: %v", err)
	}
	if strings.TrimSpace(output) != "virsh -c qemu:///system uri" {
		t.Fatalf("virsh output = %q, want fixed connection command", output)
	}
}

func TestExecCommandRunnerReportsVirshFailure(t *testing.T) {
	shimDirectory := t.TempDir()
	writeVirshShim(t, shimDirectory)
	t.Setenv("PATH", shimDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))

	_, err := (ExecCommandRunner{}).ReadConnectionURI(context.Background(), "fail")
	if err == nil {
		t.Fatal("expected virsh failure")
	}
	if !strings.Contains(err.Error(), "virsh uri failed") {
		t.Fatalf("virsh failure = %q, want command context", err.Error())
	}
}

func TestExecCommandRunnerCreatesVirshDomainFromStdin(t *testing.T) {
	installVirshShim(t)
	runner := ExecCommandRunner{}
	if err := runner.CreateDomain(context.Background(), "qemu:///system", "<domain/>"); err != nil {
		t.Fatalf("create virsh domain: %v", err)
	}
}

func TestExecCommandRunnerListsVirshDomains(t *testing.T) {
	installVirshShim(t)
	output, err := (ExecCommandRunner{}).ListDomains(context.Background(), "qemu:///system")
	if err != nil || !strings.Contains(output, "list --all --name") {
		t.Fatalf("list virsh domains: output=%q error=%v", output, err)
	}
}

func TestExecCommandRunnerReadsVirshDomainXML(t *testing.T) {
	installVirshShim(t)
	output, err := (ExecCommandRunner{}).ReadDomainXML(context.Background(), "qemu:///system", "runner")
	if err != nil || !strings.Contains(output, "dumpxml runner") {
		t.Fatalf("read virsh domain XML: output=%q error=%v", output, err)
	}
}

func TestExecCommandRunnerReadsVirshDomainState(t *testing.T) {
	installVirshShim(t)
	output, err := (ExecCommandRunner{}).ReadDomainState(context.Background(), "qemu:///system", "runner")
	if err != nil || !strings.Contains(output, "domstate runner") {
		t.Fatalf("read virsh domain state: output=%q error=%v", output, err)
	}
}

func TestExecCommandRunnerDestroysVirshDomain(t *testing.T) {
	installVirshShim(t)
	if err := (ExecCommandRunner{}).DestroyDomain(context.Background(), "qemu:///system", "runner"); err != nil {
		t.Fatalf("destroy virsh domain: %v", err)
	}
}

func installVirshShim(t *testing.T) {
	t.Helper()
	shimDirectory := t.TempDir()
	writeVirshShim(t, shimDirectory)
	t.Setenv("PATH", shimDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func writeVirshShim(t *testing.T, directory string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		contents := "@setlocal EnableDelayedExpansion\r\n@if \"%2\"==\"fail\" exit /b 1\r\n@if \"%3\"==\"create\" (set /p INPUT=& if not \"!INPUT!\"==\"<domain/>\" exit /b 2)\r\n@echo virsh %*\r\n"
		if err := os.WriteFile(filepath.Join(directory, "virsh.cmd"), []byte(contents), 0o600); err != nil {
			t.Fatalf("write virsh command shim: %v", err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate virsh test executable: %v", err)
	}
	if err := os.Link(executable, filepath.Join(directory, "virsh")); err != nil {
		t.Fatalf("install virsh test executable: %v", err)
	}
}

func runVirshShim() {
	arguments := strings.Join(os.Args[1:], " ")
	if len(os.Args) > 2 && os.Args[2] == "fail" {
		os.Exit(1)
	}
	if len(os.Args) > 3 && os.Args[3] == "create" {
		input, err := io.ReadAll(os.Stdin)
		if err != nil || string(input) != "<domain/>" {
			os.Exit(2)
		}
	}
	_, _ = fmt.Printf("virsh %s\n", arguments)
}
