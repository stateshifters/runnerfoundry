package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

func scanJob(row scanner) (core.Job, error) {
	var job core.Job
	var jobID, runnerPool string
	var enqueuedAt int64
	if err := row.Scan(&jobID, &job.Source.Adapter, &job.Source.ID, &runnerPool, &enqueuedAt); err != nil {
		return core.Job{}, fmt.Errorf("scan queued job: %w", err)
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		return core.Job{}, fmt.Errorf("parse job UUID: %w", err)
	}
	job.ID = parsedJobID
	job.RunnerPool = core.RunnerPool(runnerPool)
	job.EnqueuedAt = time.Unix(0, enqueuedAt).UTC()
	return job, nil
}

func scanProcessingJob(row scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var jobID, runnerPool, memberID string
	if err := row.Scan(&jobID, &processing.Job.Source.Adapter, &processing.Job.Source.ID,
		&runnerPool, &memberID, &processing.RunnerStarted, &processing.SourceTerminal); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan processing job: %w", err)
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		return core.ProcessingJob{}, fmt.Errorf("parse processing job UUID: %w", err)
	}
	processing.Job.ID = parsedJobID
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	processing.MemberID = core.MemberID(memberID)
	return processing, nil
}

func scanProcessingJobs(rows Rows) (jobs []core.ProcessingJob, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		job, err := scanProcessingJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan processing job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate processing jobs: %w", err)
	}
	return jobs, nil
}

func rollback(ctx context.Context, transaction Transaction, open *bool, resultErr *error) {
	if *open {
		*resultErr = errors.Join(*resultErr, transaction.Rollback(context.WithoutCancel(ctx)))
	}
}
