package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func orchardConfiguration(provider string) string {
	return "github:\n  apps:\n    - app_id: 1\n      private_key_file: app.pem\nstate:\n  path: state.db\nscale_sets:\n  - name: macos\n    max_runners: 1\n" + provider
}

func TestLoadAcceptsOrchardRunnerPool(t *testing.T) {
	root := filepath.Dir(testAbsoluteLogPath())
	for _, certificate := range []string{"", filepath.Join(root, "controller.pem")} {
		provider := fmt.Sprintf("    orchard:\n      controller_url: https://orchard.example/controller/\n      service_account: runnerfoundry\n      token_file: %q\n      vm_spec_file: %q\n      trusted_certificate_file: %q\n", filepath.Join(root, "token"), filepath.Join(root, "vm.json"), certificate)
		configured, err := Load(strings.NewReader(orchardConfiguration(provider)))
		if err != nil {
			t.Fatalf("load Orchard Runner Pool: %v", err)
		}
		kind, err := configured.RunnerPools[0].ProviderKind()
		if err != nil || kind != "orchard" || *configured.RunnerPools[0].Orchard != (OrchardConfig{ControllerURL: "https://orchard.example/controller/", ServiceAccount: "runnerfoundry", TokenFile: filepath.Join(root, "token"), VMSpecFile: filepath.Join(root, "vm.json"), TrustedCertificateFile: certificate}) {
			t.Fatalf("provider kind = %q, %v", kind, err)
		}
	}
}

func TestLoadRejectsInvalidOrchardRunnerPool(t *testing.T) {
	root := filepath.Dir(testAbsoluteLogPath())
	valid := fmt.Sprintf("    orchard:\n      controller_url: https://orchard.example\n      service_account: runnerfoundry\n      token_file: %q\n      vm_spec_file: %q\n", filepath.Join(root, "token"), filepath.Join(root, "vm.json"))
	for _, test := range []struct{ name, old, replacement, want string }{
		{"missing account", "service_account: runnerfoundry", "service_account: ''", "orchard.service_account is required"},
		{"relative token", fmt.Sprintf("token_file: %q", filepath.Join(root, "token")), "token_file: relative", "orchard.token_file must be absolute"},
		{"relative spec", fmt.Sprintf("vm_spec_file: %q", filepath.Join(root, "vm.json")), "vm_spec_file: relative", "orchard.vm_spec_file must be absolute"},
		{"relative certificate", "service_account: runnerfoundry", "service_account: runnerfoundry\n      trusted_certificate_file: relative", "orchard.trusted_certificate_file must be absolute"},
		{"missing token", fmt.Sprintf("token_file: %q", filepath.Join(root, "token")), "token_file: ''", "orchard.token_file must be absolute"},
		{"missing spec", fmt.Sprintf("vm_spec_file: %q", filepath.Join(root, "vm.json")), "vm_spec_file: ''", "orchard.vm_spec_file must be absolute"},
		{"empty query", "https://orchard.example", "https://orchard.example?", "orchard.controller_url"},
		{"empty fragment", "https://orchard.example", "https://orchard.example#", "orchard.controller_url"},
		{"missing controller", "https://orchard.example", "", "orchard.controller_url"},
		{"relative controller", "https://orchard.example", "/controller", "orchard.controller_url"},
		{"unsupported scheme", "https://orchard.example", "ftp://orchard.example", "orchard.controller_url"},
		{"missing host", "https://orchard.example", "https:///controller", "orchard.controller_url"},
		{"credentials", "https://orchard.example", "https://secret@orchard.example", "orchard.controller_url"},
		{"query", "https://orchard.example", "https://orchard.example?secret", "orchard.controller_url"},
		{"fragment", "https://orchard.example", "https://orchard.example#secret", "orchard.controller_url"},
		{"ambiguous", "    orchard:", "    docker: {}\n    orchard:", "exactly one"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(orchardConfiguration(strings.Replace(valid, test.old, test.replacement, 1))))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid configuration error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsUnsupportedTartConfiguration(t *testing.T) {
	_, err := Load(strings.NewReader(orchardConfiguration("    tart: {}\n")))
	if err == nil || !strings.Contains(err.Error(), "field tart not found") {
		t.Fatalf("obsolete provider error = %v", err)
	}
}
