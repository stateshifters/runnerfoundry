package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func scanJob(row scanner) (core.Job, error) {
	var job core.Job
	var jobID, sourceAdapter, sourceIdentifier, runnerPool string
	var enqueuedAt int64
	if err := row.Scan(&jobID, &sourceAdapter, &sourceIdentifier, &runnerPool, &enqueuedAt); err != nil {
		return core.Job{}, fmt.Errorf("scan queued job: %w", err)
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		return core.Job{}, fmt.Errorf("parse job UUID: %w", err)
	}
	job.ID = parsedJobID
	job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	job.RunnerPool = core.RunnerPool(runnerPool)
	job.EnqueuedAt = time.Unix(0, enqueuedAt).UTC()
	return job, nil
}

func scanProcessingJob(row scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var jobID, sourceAdapter, sourceIdentifier, runnerPool, memberID, runnerName string
	if err := row.Scan(&jobID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &memberID, &runnerName, &processing.RunnerStarted, &processing.SourceTerminal); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan processing job: %w", err)
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		return core.ProcessingJob{}, fmt.Errorf("parse processing job UUID: %w", err)
	}
	processing.Job.ID = parsedJobID
	processing.Job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	processing.MemberID = core.MemberID(memberID)
	processing.RunnerName = core.RunnerName(runnerName)
	return processing, nil
}

func scanClaimedProcessingJob(row scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var jobID, sourceAdapter, sourceIdentifier, runnerPool, memberID, runnerName, endpointID string
	var claimedAt int64
	if err := row.Scan(&jobID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &memberID, &runnerName, &claimedAt, &endpointID, &processing.RunnerStarted,
		&processing.SourceTerminal); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan claimed processing job: %w", err)
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		return core.ProcessingJob{}, fmt.Errorf("parse claimed processing job UUID: %w", err)
	}
	processing.Job.ID = parsedJobID
	processing.Job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	processing.MemberID = core.MemberID(memberID)
	processing.RunnerName = core.RunnerName(runnerName)
	processing.ClaimedAt = time.Unix(0, claimedAt).UTC()
	processing.InventoryEndpointID = core.InventoryEndpointID(endpointID)
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
