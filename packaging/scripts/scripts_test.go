package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDebianStateSurvivesRemoveAndIsDeletedByPurge(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Debian maintainer scripts require a POSIX shell")
	}
	root := t.TempDir()
	runScript(t, root, "postinstall.sh", "configure")
	stateDirectory := filepath.Join(root, "var", "lib", "github-scaler")
	statePath := filepath.Join(stateDirectory, "state.db")
	if err := os.WriteFile(statePath, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	runScript(t, root, "postremove.sh", "remove")
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("expected remove to preserve state: %v", err)
	}
	runScript(t, root, "postinstall.sh", "configure")
	runScript(t, root, "postremove.sh", "purge")
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("expected purge to remove state, got %v", err)
	}
}

func runScript(t *testing.T, root string, script string, action string) {
	t.Helper()
	cmd := exec.Command("sh", script, action) //nolint:gosec // Test inputs are fixed repository script names and package lifecycle actions.
	cmd.Env = append(os.Environ(), "GITHUB_SCALER_ROOT="+root)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", script, err, output)
	}
}
