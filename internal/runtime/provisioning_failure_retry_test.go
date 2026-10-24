package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestProvisioningFailureRetryErrorLeavesOutcomeUnknown(t *testing.T) {
	job := sourceOwnedJob()
	retried := false
	store := provisioningFailureStore(job, &retried)
	wantErr := errors.New("retry failed")
	store.retry = func(context.Context, core.JobID) (bool, error) {
		retried = true
		return true, wantErr
	}

	result, err := newSourceOperationsOrchestrator(
		store, failingBootstrapSource(errors.New("bootstrap failed")),
	).Process(context.Background(), job.RunnerPool)
	if !errors.Is(err, wantErr) || !retried || result.Outcome != runtimeengine.ProcessRetry ||
		result.RetryOutcome != "" || result.JobID != job.ID {
		t.Fatalf("Process() = (%#v, %v), retried=%t", result, err, retried)
	}
}
