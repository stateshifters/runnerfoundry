package core

import "testing"

func TestProvisioningFailureStatesPreserveDurableLifecyclePhases(t *testing.T) {
	if ProvisioningFailureOpen != "" || ProvisioningFailureReporting != "reporting" ||
		ProvisioningFailureReported != "reported" {
		t.Fatalf("provisioning failure states = %q, %q, %q",
			ProvisioningFailureOpen, ProvisioningFailureReporting, ProvisioningFailureReported)
	}
}
