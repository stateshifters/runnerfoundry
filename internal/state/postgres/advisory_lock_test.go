package postgres

import "testing"

func TestAdvisoryLockIdentityPreservesComponentBoundaries(t *testing.T) {
	t.Parallel()
	firstIdentity := advisoryLockIdentity("a", "bc")
	secondIdentity := advisoryLockIdentity("ab", "c")
	if firstIdentity == secondIdentity {
		t.Fatalf("ambiguous advisory lock identities: %q", firstIdentity)
	}
}
