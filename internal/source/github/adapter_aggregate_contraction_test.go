package github_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestAdapterWithdrawsQueuedFirstUntilSnapshotSurplusIsFilled(t *testing.T) {
	accepted := core.JobIdentity{JobID: uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate-2"}}
	unaccepted := core.JobIdentity{JobID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate-1"}}
	older := core.JobIdentity{JobID: uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate-0"}}
	handler := &aggregateContractionHandler{
		result: githubsource.AssignedDemandResult{ActiveJobs: 2, QueuedJobs: 1, ProcessingJobs: 1,
			SurplusQueued: []core.JobIdentity{accepted}, SurplusProcessing: []core.JobIdentity{unaccepted, older}},
		conditionalOutcomes: map[core.JobIdentity]core.ConditionalTerminalOutcome{
			accepted: core.ConditionalTerminalProtected, unaccepted: core.ConditionalTerminalApplied,
			older: core.ConditionalTerminalApplied,
		},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{{
		MessageID: 91, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}}}
	result, err := newAdapter(client, handler).Poll(context.Background(), 1)
	if err != nil || !result.Acknowledged || handler.calls != 1 {
		t.Fatalf("poll result = (%#v, %v), reconciliation calls = %d", result, err, handler.calls)
	}
	want := []core.JobIdentity{accepted, unaccepted}
	if !reflect.DeepEqual(handler.conditionalIdentities, want) {
		t.Fatalf("conditional terminal identities = %#v, want %#v", handler.conditionalIdentities, want)
	}
}

func TestAdapterTreatsInactiveSnapshotCandidateAsRemovedWithoutRereconciliation(t *testing.T) {
	identity := core.JobIdentity{JobID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate"}}
	for name, outcome := range map[string]core.ConditionalTerminalOutcome{
		"already applied": core.ConditionalTerminalAlreadyApplied,
		"missing":         core.ConditionalTerminalMissing,
	} {
		t.Run(name, func(t *testing.T) {
			handler := &aggregateContractionHandler{
				result: githubsource.AssignedDemandResult{
					ActiveJobs: 2, ProcessingJobs: 2, SurplusProcessing: []core.JobIdentity{identity},
				},
				conditionalOutcomes: map[core.JobIdentity]core.ConditionalTerminalOutcome{identity: outcome},
			}
			client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{{
				MessageID: 92, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
			}}}
			result, err := newAdapter(client, handler).Poll(context.Background(), 1)
			if err != nil || !result.Acknowledged || handler.calls != 1 || len(handler.conditionalIdentities) != 1 {
				t.Fatalf("poll replay = (%#v, %v), calls = %d, observations = %d",
					result, err, handler.calls, len(handler.conditionalIdentities))
			}
		})
	}
}

func TestAdapterLeavesMessageWhenConditionalTerminalObservationFails(t *testing.T) {
	identity := core.JobIdentity{JobID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate"}}
	for name, handler := range map[string]*aggregateContractionHandler{
		"state failure": {
			result: githubsource.AssignedDemandResult{
				ActiveJobs: 1, QueuedJobs: 1, SurplusQueued: []core.JobIdentity{identity},
			},
			conditionalErr: errors.New("state unavailable"),
		},
		"invalid outcome": {
			result: githubsource.AssignedDemandResult{
				ActiveJobs: 1, ProcessingJobs: 1, SurplusProcessing: []core.JobIdentity{identity},
			},
			conditionalOutcomes: map[core.JobIdentity]core.ConditionalTerminalOutcome{identity: "unexpected"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{{
				MessageID: 93, Statistics: &upstream.RunnerScaleSetStatistic{},
			}}}
			result, err := newAdapter(client, handler).Poll(context.Background(), 1)
			if err == nil || result.Acknowledged || len(client.acknowledged) != 0 {
				t.Fatalf("conditional terminal failure = (%#v, %v), acknowledgements = %#v",
					result, err, client.acknowledged)
			}
		})
	}
}

type aggregateContractionHandler struct {
	result                githubsource.AssignedDemandResult
	calls                 int
	conditionalOutcomes   map[core.JobIdentity]core.ConditionalTerminalOutcome
	conditionalIdentities []core.JobIdentity
	conditionalErr        error
}

func (handler *aggregateContractionHandler) ReconcileAssignedDemand(
	context.Context,
	githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	handler.calls++
	return handler.result, nil
}

func (handler *aggregateContractionHandler) ObserveTerminalIfUnaccepted(
	_ context.Context,
	identity core.JobIdentity,
) (core.ConditionalTerminalResult, error) {
	handler.conditionalIdentities = append(handler.conditionalIdentities, identity)
	return core.ConditionalTerminalResult{Outcome: handler.conditionalOutcomes[identity]}, handler.conditionalErr
}

func (*aggregateContractionHandler) IngestDemand(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{}, nil
}

func (*aggregateContractionHandler) ObserveRunnerAcceptance(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}

func (*aggregateContractionHandler) ObserveTerminal(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
