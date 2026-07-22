//go:build integration

package docker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMain(testSuite *testing.M) {
	if strings.EqualFold(filepath.Base(os.Args[0]), "docker") || strings.EqualFold(filepath.Base(os.Args[0]), "docker.exe") {
		runDockerShim()
		return
	}
	os.Exit(testSuite.Run())
}

func TestExecCommandRunnerInvokesDockerFromPath(t *testing.T) {
	shimDirectory := t.TempDir()
	writeDockerShim(t, shimDirectory)
	t.Setenv("PATH", shimDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))

	output, err := (ExecCommandRunner{}).ListComposeServices(context.Background(), ComposeConfig{
		ComposeFile: "compose.yml",
	})
	if err != nil {
		t.Fatalf("run docker shim: %v", err)
	}
	if strings.TrimSpace(output) != "docker compose -f compose.yml config --services" {
		t.Fatalf("docker output = %q, want compose service command", output)
	}
}

func TestExecCommandRunnerRedactsJITConfigFromDockerFailure(t *testing.T) {
	shimDirectory := t.TempDir()
	writeDockerShim(t, shimDirectory)
	t.Setenv("PATH", shimDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))

	err := (ExecCommandRunner{}).StartComposeRunner(context.Background(), ComposeConfig{Service: "runner"}, StartRequest{
		ScaleSetName: "linux", RunnerName: "linux-1", JITConfig: "opaque-jit-value",
	})
	if err == nil {
		t.Fatal("expected docker failure")
	}
	if strings.Contains(err.Error(), "opaque-jit-value") {
		t.Fatalf("docker failure disclosed JIT config: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "docker diagnostic: [REDACTED]") {
		t.Fatalf("docker failure = %q, want redacted diagnostic", err.Error())
	}
}

func TestExecCommandRunnerReportsMissingDocker(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := (ExecCommandRunner{}).ListComposeRunners(context.Background(), "linux"); err == nil {
		t.Fatal("expected missing docker failure")
	}
}

func TestExecCommandRunnerRemovesDockerContainer(t *testing.T) {
	shimDirectory := t.TempDir()
	writeDockerShim(t, shimDirectory)
	t.Setenv("PATH", shimDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := (ExecCommandRunner{}).RemoveComposeRunner(context.Background(), "runner-id"); err != nil {
		t.Fatalf("remove docker container: %v", err)
	}
}

func writeDockerShim(t *testing.T, directory string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		contents := "@echo %* | findstr opaque-jit-value >nul && (echo docker diagnostic: opaque-jit-value 1>&2 & exit /b 1)\r\n@echo docker %*\r\n"
		if err := os.WriteFile(filepath.Join(directory, "docker.cmd"), []byte(contents), 0o600); err != nil {
			t.Fatalf("write docker command shim: %v", err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate docker test executable: %v", err)
	}
	if err := os.Link(executable, filepath.Join(directory, "docker")); err != nil {
		t.Fatalf("install docker test executable: %v", err)
	}
}

func runDockerShim() {
	arguments := strings.Join(os.Args[1:], " ")
	if strings.Contains(arguments, "opaque-jit-value") {
		_, _ = fmt.Fprintln(os.Stderr, "docker diagnostic: opaque-jit-value")
		os.Exit(1)
	}
	_, _ = fmt.Printf("docker %s\n", arguments)
}
