package supervisor

import (
	"errors"
	"testing"
)

func TestReportedActivityErrorPreservesReportedFailure(t *testing.T) {
	wantErr := errors.New("reported failure")
	reported := markOperationallyReported(wantErr)
	if reported.Error() != wantErr.Error() || !errors.Is(reported, wantErr) {
		t.Fatalf("reported error = %v, want wrapped %v", reported, wantErr)
	}
	marker, ok := reported.(interface{ OperationallyReported() bool })
	if !ok || !marker.OperationallyReported() {
		t.Fatalf("reported error marker = %#v", reported)
	}
	if markOperationallyReported(nil) != nil {
		t.Fatal("nil error was marked as reported")
	}
}
