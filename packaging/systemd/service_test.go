package systemd

import (
	_ "embed"
	"strings"
	"testing"
)

//go:embed runnerfoundry.service
var runnerFoundryService string

func TestRunnerFoundryServiceProvisionsLogDirectory(t *testing.T) {
	for _, expectedDirective := range []string{
		"LogsDirectory=runnerfoundry",
		"LogsDirectoryMode=0750",
	} {
		if !containsServiceDirective(runnerFoundryService, expectedDirective) {
			t.Errorf("service unit does not contain %q", expectedDirective)
		}
	}
}

func containsServiceDirective(serviceContents string, expectedDirective string) bool {
	for line := range strings.SplitSeq(serviceContents, "\n") {
		if strings.TrimSpace(line) == expectedDirective {
			return true
		}
	}
	return false
}
