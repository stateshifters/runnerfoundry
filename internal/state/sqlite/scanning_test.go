package sqlite

import (
	"context"
	"errors"
	"testing"
)

func TestJobScannersRejectInvalidPersistedUUIDs(t *testing.T) {
	t.Parallel()
	invalidJob := queuedJobValues()
	invalidJob[0] = "invalid"
	if _, err := scanJob(rowFromValues(invalidJob)); err == nil {
		t.Fatal("expected invalid job UUID")
	}
	invalidProcessingJob := processingJobValues()
	invalidProcessingJob[0] = "invalid"
	if _, err := scanProcessingJob(rowFromValues(invalidProcessingJob)); err == nil {
		t.Fatal("expected invalid processing job UUID")
	}
	invalidClaimedProcessingJob := []any{"invalid", "github", "request-3", "linux", testMemberIDText,
		"external-runner", fixedTime().UnixNano(), "docker:member-a", false, false, ""}
	if _, err := scanClaimedProcessingJob(rowFromValues(invalidClaimedProcessingJob)); err == nil {
		t.Fatal("expected invalid claimed processing job UUID")
	}
}

func TestRollbackReportsProtocolFailureAndIgnoresClosedTransaction(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("protocol")
	var resultErr error
	open := true
	rollback(context.Background(), Transaction{rollback: failingCompletion(wantErr)}, &open, &resultErr)
	if !errors.Is(resultErr, wantErr) {
		t.Fatalf("rollback error = %v", resultErr)
	}
	open = false
	rollback(context.Background(), Transaction{}, &open, &resultErr)
}
