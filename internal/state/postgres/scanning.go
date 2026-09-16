package postgres

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func scanQueuedJob(row Scanner) (core.Job, error) {
	var job core.Job
	var sourceAdapter, sourceIdentifier, runnerPool string
	if err := row.Scan(&job.ID, &sourceAdapter, &sourceIdentifier, &runnerPool, &job.EnqueuedAt); err != nil {
		return core.Job{}, fmt.Errorf("scan queued job: %w", err)
	}
	job.RunnerPool = core.RunnerPool(runnerPool)
	job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	return job, nil
}

func scanClaimedProcessingJob(row Scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var runnerAccepted bool
	var sourceAdapter, sourceIdentifier, runnerPool, runnerName, provisioningFailure string
	if err := row.Scan(&processing.Job.ID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &processing.MemberID, &runnerName,
		&processing.ClaimedAt, &processing.InventoryEndpointID,
		&runnerAccepted, &processing.SourceTerminal, &provisioningFailure); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan claimed processing job: %w", err)
	}
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	processing.RunnerName = core.RunnerName(runnerName)
	processing.Job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	processing.RunnerAccepted = runnerAccepted
	processing.ProvisioningFailure = core.ProvisioningFailureState(provisioningFailure)
	return processing, nil
}

func readQueuedJob(ctx context.Context, transaction Transaction, statement string, arguments ...any) (core.Job, error) {
	var job core.Job
	err := transaction.ReadRow(ctx, statement, func(row Scanner) error {
		var err error
		job, err = scanQueuedJob(row)
		return err
	}, arguments...)
	if err != nil {
		return core.Job{}, fmt.Errorf("read persisted job: %w", err)
	}
	return job, nil
}

func readClaimedProcessingJob(
	ctx context.Context,
	transaction Transaction,
	statement string,
	arguments ...any,
) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	err := transaction.ReadRow(ctx, statement, func(row Scanner) error {
		var err error
		processing, err = scanClaimedProcessingJob(row)
		return err
	}, arguments...)
	if err != nil {
		return core.ProcessingJob{}, fmt.Errorf("read persisted processing job: %w", err)
	}
	return processing, nil
}

func readClaimedProcessingJobs(
	ctx context.Context,
	transaction Transaction,
	statement string,
	arguments ...any,
) ([]core.ProcessingJob, error) {
	var jobs []core.ProcessingJob
	err := transaction.VisitRows(ctx, statement, func(row Scanner) error {
		job, err := scanClaimedProcessingJob(row)
		if err != nil {
			return err
		}
		jobs = append(jobs, job)
		return nil
	}, arguments...)
	if err != nil {
		return nil, fmt.Errorf("read processing jobs: %w", err)
	}
	return jobs, nil
}
