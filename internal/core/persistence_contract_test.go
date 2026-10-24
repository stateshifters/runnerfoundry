package core_test

import (
	"errors"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"testing"
	"time"
)

func TestJobRequestRequiresCompletePersistenceIdentity(t *testing.T) {
	t.Parallel()
	complete := core.JobRequest{Source: core.SourceReference{Adapter: "source", ID: "request"}, RunnerPool: "linux", EnqueuedAt: time.Unix(100, 0)}
	for name, invalidate := range map[string]func(*core.JobRequest){
		"missing adapter":         func(request *core.JobRequest) { request.Source.Adapter = "" },
		"missing source identity": func(request *core.JobRequest) { request.Source.ID = "" },
		"missing pool":            func(request *core.JobRequest) { request.RunnerPool = "" },
		"missing enqueue time":    func(request *core.JobRequest) { request.EnqueuedAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			request := complete
			invalidate(&request)
			if err := request.Validate(); !errors.Is(err, core.ErrInvalidJobRequest) {
				t.Fatalf("incomplete request validation = %v", err)
			}
		})
	}
	if err := complete.Validate(); err != nil {
		t.Fatalf("complete request validation = %v", err)
	}
}

func TestProcessingClaimRequiresCallerOwnedAttemptEvidence(t *testing.T) {
	t.Parallel()
	complete := core.ProcessingClaim{MemberID: "member", AttemptID: core.AttemptID{1}, ClaimedAt: time.Unix(100, 0), InventoryEndpointID: "endpoint"}
	for name, invalidate := range map[string]func(*core.ProcessingClaim){
		"missing member":     func(claim *core.ProcessingClaim) { claim.MemberID = "" },
		"missing attempt":    func(claim *core.ProcessingClaim) { claim.AttemptID = core.AttemptID{} },
		"missing claim time": func(claim *core.ProcessingClaim) { claim.ClaimedAt = time.Time{} },
		"missing endpoint":   func(claim *core.ProcessingClaim) { claim.InventoryEndpointID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			claim := complete
			invalidate(&claim)
			if err := claim.Validate(); !errors.Is(err, core.ErrInvalidProcessingClaim) {
				t.Fatalf("incomplete claim validation = %v", err)
			}
		})
	}
	if err := complete.Validate(); err != nil {
		t.Fatalf("complete claim validation = %v", err)
	}
}

func TestJobRequestScopeMustBeCompleteAndBelongToItsAdapter(t *testing.T) {
	t.Parallel()
	for name, scenario := range map[string]struct {
		scope core.DemandScope
		valid bool
	}{
		"unscoped":               {valid: true},
		"matching scope":         {scope: core.DemandScope{Adapter: "source", ID: "queue"}, valid: true},
		"missing scope identity": {scope: core.DemandScope{Adapter: "source"}},
		"missing scope adapter":  {scope: core.DemandScope{ID: "queue"}},
		"different adapter":      {scope: core.DemandScope{Adapter: "other", ID: "queue"}},
	} {
		t.Run(name, func(t *testing.T) {
			request := core.JobRequest{Source: core.SourceReference{Adapter: "source", ID: "request"},
				Scope: scenario.scope, RunnerPool: "linux", EnqueuedAt: time.Unix(100, 0)}
			err := request.Validate()
			if scenario.valid && err != nil || !scenario.valid && !errors.Is(err, core.ErrInvalidJobRequest) {
				t.Fatalf("request scope validation = %v, valid = %v", err, scenario.valid)
			}
		})
	}
}
