package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestRetryUnstartedProcessingRejectsReplacementClaim(t *testing.T) {
	stale := recoveryProcessing()
	replacement := stale
	replacement.RunnerName = core.AttemptRunnerName(stale.Job.ID, core.AttemptID{2})
	replacement.ClaimedAt = stale.ClaimedAt.Add(time.Minute)
	replacement.InventoryEndpointID = "docker:member-b"
	protocol := &recordedTransaction{rows: []Row{rowFromValues(postgresClaimedOutcomeValues(replacement, false, false))}}

	result, err := NewStore(protocol.executor()).RetryUnstarted(context.Background(), stale)
	if err != nil || result.Outcome != core.RetryClaimChanged || protocol.commitCalls != 1 {
		t.Fatalf("stale retry = (%#v, %v), statements=%#v arguments=%#v",
			result, err, protocol.statements, protocol.arguments)
	}
	if len(protocol.statements) != 1 || !strings.Contains(protocol.statements[0], "claimed_at") {
		t.Fatalf("stale retry protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestRetryUnstartedProcessingClassifiesLostRequeue(t *testing.T) {
	processing := recoveryProcessing()
	plain := rowFromValues(postgresClaimedOutcomeValues(processing, false, false))
	protocol := &recordedTransaction{rows: []Row{plain, noPostgresRow(), plain}}
	result, err := NewStore(protocol.executor()).RetryUnstarted(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryClaimChanged {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestRetryUnstartedProcessingReportsLostRequeueClassificationFailure(t *testing.T) {
	processing := recoveryProcessing()
	expectedErr := errors.New("protocol")
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues(postgresClaimedOutcomeValues(processing, false, false)),
		noPostgresRow(),
		{scan: func(...any) error { return expectedErr }},
	}}
	if _, err := NewStore(protocol.executor()).RetryUnstarted(context.Background(), processing); !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestRetryUnstartedProcessingClassifiesReplacementAfterLostRequeue(t *testing.T) {
	processing := recoveryProcessing()
	replacement := processing
	replacement.RunnerName = core.AttemptRunnerName(processing.Job.ID, core.AttemptID{2})
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues(postgresClaimedOutcomeValues(processing, false, false)),
		noPostgresRow(),
		rowFromValues(postgresClaimedOutcomeValues(replacement, false, false)),
	}}
	result, err := NewStore(protocol.executor()).RetryUnstarted(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryClaimChanged {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestRecoverUnstartedProcessingClassifiesReplacementBeforeLifecycle(t *testing.T) {
	stale := retryProcessingJob()
	replacement := stale
	replacement.RunnerName = core.AttemptRunnerName(stale.Job.ID, core.AttemptID{2})
	replacement.ClaimedAt = stale.ClaimedAt.Add(time.Minute)
	replacement.InventoryEndpointID = "docker:member-b"
	for name, state := range map[string]struct {
		accepted bool
		terminal bool
		failure  core.ProvisioningFailureState
	}{
		"accepted":  {accepted: true},
		"terminal":  {terminal: true},
		"reporting": {failure: core.ProvisioningFailureReporting},
		"reported":  {failure: core.ProvisioningFailureReported},
	} {
		t.Run(name, func(t *testing.T) {
			values := postgresClaimedOutcomeValues(replacement, state.accepted, state.terminal)
			values[10] = string(state.failure)
			protocol := &recordedTransaction{rows: []Row{noPostgresRow(), rowFromValues(values)}}

			result, err := NewStore(protocol.executor()).RecoverUnstartedProcessing(context.Background(), stale)
			if err != nil || result.Outcome != core.RetryClaimChanged || protocol.commitCalls != 1 {
				t.Fatalf("recovery = (%#v, %v), statements=%#v", result, err, protocol.statements)
			}
		})
	}
}
