package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func scanQueuedJob(row scanner) (core.Job, error) {
	var job core.Job
	var sourceAdapter, sourceIdentifier, runnerPool string
	if err := row.Scan(&job.ID, &sourceAdapter, &sourceIdentifier, &runnerPool, &job.EnqueuedAt); err != nil {
		return core.Job{}, fmt.Errorf("scan queued job: %w", err)
	}
	job.RunnerPool = core.RunnerPool(runnerPool)
	job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	return job, nil
}

func scanProcessingJob(row scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var sourceAdapter, sourceIdentifier, runnerPool, runnerName string
	if err := row.Scan(&processing.Job.ID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &processing.MemberID, &runnerName,
		&processing.RunnerStarted, &processing.SourceTerminal); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan processing job: %w", err)
	}
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	processing.RunnerName = core.RunnerName(runnerName)
	processing.Job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	return processing, nil
}

func scanClaimedProcessingJob(row scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var sourceAdapter, sourceIdentifier, runnerPool, runnerName string
	if err := row.Scan(&processing.Job.ID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &processing.MemberID, &runnerName,
		&processing.ClaimedAt, &processing.InventoryEndpointID,
		&processing.RunnerStarted, &processing.SourceTerminal); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan claimed processing job: %w", err)
	}
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	processing.RunnerName = core.RunnerName(runnerName)
	processing.Job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	return processing, nil
}

func scanClaimedProcessingJobs(rows Rows) (jobs []core.ProcessingJob, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		job, err := scanClaimedProcessingJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed processing jobs: %w", err)
	}
	return jobs, nil
}

func rollback(ctx context.Context, transaction Transaction, open *bool, resultErr *error) {
	if *open {
		*resultErr = errors.Join(*resultErr, transaction.Rollback(context.WithoutCancel(ctx)))
	}
}
