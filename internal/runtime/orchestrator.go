package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

var ErrAwaitingClaim = errors.New("job is awaiting a local capacity claim")

// SourceAcquirer accepts a claimed job through the leader-owned source session.
type SourceAcquirer interface {
	Acquire(ctx context.Context, job core.Job) error
}

// BootstrapGenerator obtains source-owned initialization data for an acquired
// job through an ordinary source client.
type BootstrapGenerator interface {
	GenerateBootstrap(ctx context.Context, job core.Job, runnerName string) (execution.Bootstrap, error)
}

type SourceAdapter interface {
	SourceAcquirer
	BootstrapGenerator
	core.ResultReporter
}

// Orchestrator coordinates one source-neutral job attempt. Durable job state is
// owned by the core repository, while source acquisition and runner provisioning
// remain behind their respective boundaries.
type Orchestrator struct {
	store       core.JobStore
	source      SourceAdapter
	provider    execution.ExecutionProvider
	member      core.MemberID
	currentTime func() time.Time
}

// NewOrchestrator constructs an orchestration boundary for one member. The clock
// supplies claim timestamps and should return UTC time in production.
func NewOrchestrator(
	store core.JobStore,
	source SourceAdapter,
	provider execution.ExecutionProvider,
	member core.MemberID,
	currentTime func() time.Time,
) *Orchestrator {
	return &Orchestrator{
		store: store, source: source, provider: provider, member: member, currentTime: currentTime,
	}
}

// Request records one source request. Repeated requests are idempotent at the
// core repository boundary and return the existing job record.
func (orchestrator *Orchestrator) Request(
	ctx context.Context,
	request core.JobRequest,
) error {
	record, _, err := orchestrator.store.Enqueue(ctx, request)
	if err != nil {
		return fmt.Errorf("enqueue source request %q: %w", request.Source.ID, err)
	}
	if record.Job.ID == (core.JobID{}) {
		finalized, receiptErr := orchestrator.alreadyFinalized(ctx, request.Source)
		if receiptErr != nil {
			return receiptErr
		}
		if finalized {
			return nil
		}
		return fmt.Errorf("source request %q has no durable job or receipt", request.Source.ID)
	}
	if record.State == core.JobStateQueued {
		return ErrAwaitingClaim
	}
	if record.State == core.JobStateResultPending || !record.AcquiredAt.IsZero() {
		return nil
	}
	if !record.AcquisitionStartedAt.IsZero() {
		return orchestrator.terminalizeInterruptedAcquisition(ctx, record)
	}

	return orchestrator.acquireClaimedJob(ctx, record)
}

// Started records a source-observed runner start. Events redelivered after the
// job was finalized are acknowledged without recreating internal state.
func (orchestrator *Orchestrator) Started(
	ctx context.Context,
	source core.SourceReference,
	startedAt time.Time,
) error {
	finalized, err := orchestrator.alreadyFinalized(ctx, source)
	if err != nil || finalized {
		return err
	}

	if _, err = orchestrator.store.MarkStartedBySource(ctx, source, startedAt); err != nil {
		return fmt.Errorf("mark source request %q started: %w", source.ID, err)
	}

	return nil
}

// Completed persists and delivers a source-observed terminal result. Events
// redelivered after finalization are acknowledged without reporting twice.
func (orchestrator *Orchestrator) Completed(
	ctx context.Context,
	source core.SourceReference,
	result core.Result,
) error {
	finalized, err := orchestrator.alreadyFinalized(ctx, source)
	if err != nil || finalized {
		return err
	}

	return orchestrator.Complete(ctx, source, result)
}

// ClaimWork claims at most one job for the requested Runner Pool. Acquisition
// and runner start are deliberately separate leader/member stages.
func (orchestrator *Orchestrator) ClaimWork(ctx context.Context, runnerPool core.RunnerPool) (bool, error) {
	_, claimed, err := orchestrator.store.ClaimOldest(ctx, runnerPool, orchestrator.member, orchestrator.currentTime())
	if err != nil {
		return false, fmt.Errorf("claim work for Runner Pool %q: %w", runnerPool, err)
	}
	if !claimed {
		return false, nil
	}

	return true, nil
}

// Complete persists a source-observed terminal result before delivering it and
// finalizing the internal job. A delivery failure leaves the job result pending.
func (orchestrator *Orchestrator) Complete(
	ctx context.Context,
	source core.SourceReference,
	result core.Result,
) error {
	record, err := orchestrator.store.SetResultPendingBySource(ctx, source, result)
	if err != nil {
		return fmt.Errorf("persist result for source request %q: %w", source.ID, err)
	}

	return orchestrator.deliverResult(ctx, record, result)
}

// RetryResult retries only source delivery for an already pending result. It
// does not claim work, reacquire bootstrap data, or start another runner.
func (orchestrator *Orchestrator) RetryResult(ctx context.Context, jobID core.JobID) error {
	record, found, err := orchestrator.store.Lookup(ctx, jobID)
	if err != nil {
		return fmt.Errorf("look up pending result for job %s: %w", jobID, err)
	}
	if !found {
		return fmt.Errorf("retry result for job %s: job not found", jobID)
	}
	if record.State != core.JobStateResultPending || record.Result == nil {
		return fmt.Errorf("retry result for job %s: result is not pending", jobID)
	}

	return orchestrator.deliverResult(ctx, record, *record.Result)
}

// RetryPendingResults recovers durable result delivery after transient failure
// or restart. Every pending job is attempted independently; failed deliveries
// remain pending and do not prevent later jobs in the batch from finalizing.
func (orchestrator *Orchestrator) RetryPendingResults(ctx context.Context, limit int) (int, error) {
	records, err := orchestrator.store.PendingResults(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("list pending results: %w", err)
	}

	delivered := 0
	deliveryErrors := make([]error, 0)
	for _, record := range records {
		if record.Result == nil {
			deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver pending job %s: result is missing", record.Job.ID))
			continue
		}
		if err = orchestrator.deliverResult(ctx, record, *record.Result); err != nil {
			deliveryErrors = append(deliveryErrors, err)
			continue
		}
		delivered++
	}

	return delivered, errors.Join(deliveryErrors...)
}

// FailExpiredMember moves every attempt owned by an expired member to the
// normal failure-result path, then independently attempts each delivery.
func (orchestrator *Orchestrator) FailExpiredMember(ctx context.Context, member core.MemberID) (int, error) {
	result := core.Result{
		Outcome: core.OutcomeFailure,
		Origin:  core.ResultOriginService,
		Reason:  fmt.Sprintf("RunnerFoundry member %q expired during the job attempt", member),
	}
	records, err := orchestrator.store.SetMemberAttemptsResultPending(ctx, member, result)
	if err != nil {
		return 0, fmt.Errorf("fail attempts for expired member %q: %w", member, err)
	}

	return len(records), orchestrator.deliverResults(ctx, records, result)
}

// FailUnstartedAttempts fails attempts that have not received a source start
// event within the configured timeout. Started attempts are excluded by core.
func (orchestrator *Orchestrator) FailUnstartedAttempts(
	ctx context.Context,
	runnerPool core.RunnerPool,
	jobStartTimeout time.Duration,
) (int, error) {
	result := core.Result{
		Outcome: core.OutcomeFailure,
		Origin:  core.ResultOriginService,
		Reason:  "runner did not start before the configured timeout",
	}
	before := orchestrator.currentTime().Add(-jobStartTimeout)
	records, err := orchestrator.store.SetUnstartedAttemptsResultPending(ctx, runnerPool, before, result)
	if err != nil {
		return 0, fmt.Errorf("fail unstarted attempts for Runner Pool %q before %s: %w", runnerPool, before, err)
	}

	return len(records), orchestrator.deliverResults(ctx, records, result)
}

// CleanupUnserviceable sends a queued job through the same terminal failure
// path as an attempted job; it never creates replacement demand.
func (orchestrator *Orchestrator) CleanupUnserviceable(ctx context.Context, jobID core.JobID, reason string) error {
	job := core.Job{ID: jobID}
	return orchestrator.completeAttempt(ctx, job, errors.New(reason))
}

func (orchestrator *Orchestrator) completeAttempt(ctx context.Context, job core.Job, attemptErr error) error {
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: attemptErr.Error()}
	record, err := orchestrator.store.SetResultPending(ctx, job.ID, result)
	if err != nil {
		return fmt.Errorf("persist result for job %s: %w", job.ID, err)
	}

	return orchestrator.deliverResult(ctx, record, result)
}

func (orchestrator *Orchestrator) deliverResult(ctx context.Context, record core.JobRecord, result core.Result) error {
	disposition, err := orchestrator.source.ReportResult(ctx, record.Job, result)
	if err != nil {
		return fmt.Errorf("report result for job %s: %w", record.Job.ID, err)
	}

	if _, err = orchestrator.store.Finalize(ctx, record.Job.ID, disposition); err != nil {
		return fmt.Errorf("finalize job %s: %w", record.Job.ID, err)
	}

	return nil
}

func (orchestrator *Orchestrator) deliverResults(
	ctx context.Context,
	records []core.JobRecord,
	result core.Result,
) error {
	deliveryErrors := make([]error, 0, len(records))
	for _, record := range records {
		if err := orchestrator.deliverResult(ctx, record, result); err != nil {
			deliveryErrors = append(deliveryErrors, err)
		}
	}

	return errors.Join(deliveryErrors...)
}

func (orchestrator *Orchestrator) alreadyFinalized(ctx context.Context, source core.SourceReference) (bool, error) {
	_, found, err := orchestrator.store.LookupReceipt(ctx, source)
	if err != nil {
		return false, fmt.Errorf("look up receipt for source request %q: %w", source.ID, err)
	}

	return found, nil
}
