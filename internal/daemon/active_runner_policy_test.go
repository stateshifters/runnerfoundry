package daemon

import "testing"

func TestActiveRunnerPolicyDefaultsAndValidates(t *testing.T) {
	t.Parallel()

	if policy, err := resolveActiveRunnerPolicy(""); err != nil || policy != LeaveActiveRunners {
		t.Fatalf("default policy = %q, %v", policy, err)
	}
	if policy, err := resolveActiveRunnerPolicy(TerminateActiveRunners); err != nil || policy != TerminateActiveRunners {
		t.Fatalf("terminate policy = %q, %v", policy, err)
	}
	if _, err := resolveActiveRunnerPolicy("destroy"); err == nil {
		t.Fatal("invalid active-runner policy was accepted")
	}
}
