package buildinfo

import "testing"

func TestVersionDefaultsToDevelopmentIdentity(t *testing.T) {
	if Version() != "dev" {
		t.Fatalf("version = %q, want dev", Version())
	}
}
