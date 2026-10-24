package runtime_test

import (
	"testing"
	"time"
)

func TestReconcilePoolUsesConfiguredStartTimeout(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		timeout     time.Duration
		age         time.Duration
		accepted    bool
		wantRecover bool
	}{
		{name: "shorter timeout", timeout: time.Minute, age: time.Minute, wantRecover: true},
		{name: "longer timeout", timeout: 10 * time.Minute, age: 6 * time.Minute},
		{name: "just before boundary", timeout: 10 * time.Minute, age: 10*time.Minute - time.Nanosecond},
		{name: "at boundary", timeout: 10 * time.Minute, age: 10 * time.Minute, wantRecover: true},
		{name: "accepted runner retained", timeout: time.Minute, age: 2 * time.Minute, accepted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, recovered, err := recoveryDecision(now, now.Add(-test.age), "test:endpoint", test.accepted, test.timeout)
			if err != nil || recovered != test.wantRecover {
				t.Fatalf("ReconcilePool() = (%#v, %v), recovered=%t, want %t", result, err, recovered, test.wantRecover)
			}
		})
	}
}
