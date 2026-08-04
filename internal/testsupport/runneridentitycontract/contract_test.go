package runneridentitycontract

import "testing"

func TestCasesDescribeEveryRequiredOutcome(t *testing.T) {
	seen := make(map[Outcome]bool)
	for _, contractCase := range Cases() {
		if contractCase.Name == "" {
			t.Fatal("contract case has no name")
		}
		seen[contractCase.Outcome] = true
	}
	for _, outcome := range []Outcome{Success, Invalid, NotFound, Mismatch, Invariant} {
		if !seen[outcome] {
			t.Fatalf("contract cases omit outcome %q", outcome)
		}
	}
}
