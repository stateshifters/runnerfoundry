package daemon

import (
	"testing"
	"time"
)

func TestHealthyDiscoveryUsesConfiguredInterval(t *testing.T) {
	configuredInterval := 10 * time.Minute
	delay := nextReconciliationDelay(activeConfiguration{}, configuredInterval, time.Second)
	if delay != configuredInterval {
		t.Fatalf("reconciliation delay = %s, want %s", delay, configuredInterval)
	}
}
