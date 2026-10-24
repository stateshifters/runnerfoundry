package core

import "testing"

func TestBootstrapOwnsContentAcrossConstructionAndReads(t *testing.T) {
	content := []byte("opaque")
	bootstrap := NewBootstrap(content)
	content[0] = 'x'

	firstRead := bootstrap.Content()
	if string(firstRead) != "opaque" {
		t.Fatalf("Content() = %q", firstRead)
	}
	firstRead[0] = 'x'
	if secondRead := bootstrap.Content(); string(secondRead) != "opaque" {
		t.Fatalf("second Content() = %q", secondRead)
	}
}

func TestZeroBootstrapHasNoContent(t *testing.T) {
	if content := (Bootstrap{}).Content(); len(content) != 0 {
		t.Fatalf("len(Content()) = %d, want 0", len(content))
	}
}
