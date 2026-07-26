package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const packagingDescription = "Runner Foundry shared control plane for self-hosted ephemeral CI runners"

const postinstallCommandBoundary = `
getent() {
	case ",$EXISTING_GROUPS," in
		*",$2,"*) return 0 ;;
	esac
	return 2
}
id() { return 1; }
record_command() { printf '%s\n' "$*" >> "$COMMAND_LOG"; }
groupadd() { record_command "groupadd $*"; }
useradd() { record_command "useradd $*"; }
usermod() { record_command "usermod $*"; }
install() { record_command "install $*"; }
chown() { record_command "chown $*"; }
chmod() { record_command "chmod $*"; }
systemctl() { record_command "systemctl $*"; }
. ./postinstall.sh
`

func TestPackagingUsesRunnerFoundryProductName(t *testing.T) {
	nfpmContents, err := os.ReadFile(filepath.Join("..", "..", "nfpm.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	serviceContents, err := os.ReadFile(filepath.Join("..", "systemd", "runnerfoundry.service"))
	if err != nil {
		t.Fatal(err)
	}
	assertContentsContainLine(t, nfpmContents, "description: "+packagingDescription)
	assertContentsContainLine(t, serviceContents, "Description="+packagingDescription)
}

type providerGroupAccessTestCase struct {
	name           string
	existingGroups string
	expectedTrace  string
}

var providerGroupAccessTestCases = []providerGroupAccessTestCase{
	{
		name:           "both provider groups",
		existingGroups: "runnerfoundry,docker,libvirt",
		expectedTrace: "useradd --system --gid runnerfoundry --home-dir /nonexistent --shell /usr/sbin/nologin runnerfoundry\n" +
			"usermod --append --groups docker runnerfoundry\n" +
			"usermod --append --groups libvirt runnerfoundry\n",
	},
	{
		name:           "docker only",
		existingGroups: "runnerfoundry,docker",
		expectedTrace: "useradd --system --gid runnerfoundry --home-dir /nonexistent --shell /usr/sbin/nologin runnerfoundry\n" +
			"usermod --append --groups docker runnerfoundry\n",
	},
	{
		name:           "libvirt only",
		existingGroups: "runnerfoundry,libvirt",
		expectedTrace: "useradd --system --gid runnerfoundry --home-dir /nonexistent --shell /usr/sbin/nologin runnerfoundry\n" +
			"usermod --append --groups libvirt runnerfoundry\n",
	},
	{
		name:           "no provider groups",
		existingGroups: "runnerfoundry",
		expectedTrace:  "useradd --system --gid runnerfoundry --home-dir /nonexistent --shell /usr/sbin/nologin runnerfoundry\n",
	},
}

func TestPostinstallGrantsExistingProviderGroupAccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Debian maintainer scripts require a POSIX shell")
	}

	for _, testCase := range providerGroupAccessTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualTrace := runPostinstallWithCommandTrace(t, testCase.existingGroups)
			if actualTrace != testCase.expectedTrace {
				t.Fatalf("postinstall account command snapshot mismatch\nexpected:\n%sactual:\n%s", testCase.expectedTrace, actualTrace)
			}
		})
	}
}

func TestDebianStateSurvivesRemoveAndIsDeletedByPurge(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Debian maintainer scripts require a POSIX shell")
	}
	root := t.TempDir()
	runScript(t, root, exec.Command("sh", "postinstall.sh", "configure"))
	stateDirectory := filepath.Join(root, "var", "lib", "runnerfoundry")
	statePath := filepath.Join(stateDirectory, "state.db")
	if err := os.WriteFile(statePath, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	runScript(t, root, exec.Command("sh", "postremove.sh", "remove"))
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("expected remove to preserve state: %v", err)
	}
	runScript(t, root, exec.Command("sh", "postinstall.sh", "configure"))
	runScript(t, root, exec.Command("sh", "postremove.sh", "purge"))
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("expected purge to remove state, got %v", err)
	}
}

func runScript(t *testing.T, root string, cmd *exec.Cmd) {
	t.Helper()
	cmd.Env = append(os.Environ(), "RUNNERFOUNDRY_ROOT="+root)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", cmd.String(), err, output)
	}
}

func assertContentsContainLine(t *testing.T, contents []byte, expectedLine string) {
	t.Helper()
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.TrimSuffix(line, "\r") == expectedLine {
			return
		}
	}
	t.Fatalf("contents do not contain snapshot line %q", expectedLine)
}

func runPostinstallWithCommandTrace(t *testing.T, existingGroups string) string {
	t.Helper()
	commandLogDirectory := t.TempDir()
	executePostinstall(t, filepath.Join(commandLogDirectory, "commands.log"), existingGroups)
	return readAccountCommandTrace(t, commandLogDirectory)
}

func executePostinstall(t *testing.T, commandLog string, existingGroups string) {
	t.Helper()
	cmd := exec.Command("sh", "-c", postinstallCommandBoundary)
	cmd.Env = append(os.Environ(),
		"RUNNERFOUNDRY_ROOT=",
		"EXISTING_GROUPS="+existingGroups,
		"COMMAND_LOG="+commandLog,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", cmd.String(), err, output)
	}
}

func readAccountCommandTrace(t *testing.T, commandLogDirectory string) string {
	t.Helper()
	commandLogRoot, err := os.OpenRoot(commandLogDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := commandLogRoot.Close(); closeErr != nil {
			t.Errorf("close command log root: %v", closeErr)
		}
	})
	trace, err := commandLogRoot.ReadFile("commands.log")
	if err != nil {
		t.Fatal(err)
	}

	accountCommands := make([]string, 0, 3)
	for _, line := range strings.Split(string(trace), "\n") {
		if strings.HasPrefix(line, "useradd ") || strings.HasPrefix(line, "usermod ") {
			accountCommands = append(accountCommands, line)
		}
	}
	if len(accountCommands) == 0 {
		return ""
	}
	return strings.Join(accountCommands, "\n") + "\n"
}
