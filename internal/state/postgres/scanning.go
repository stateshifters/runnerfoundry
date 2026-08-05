package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func scanQueuedJob(row scanner) (core.Job, error) {
	var job core.Job
	var runnerPool string
	if err := row.Scan(&job.ID, &job.Source.Adapter, &job.Source.ID, &runnerPool, &job.EnqueuedAt); err != nil {
		return core.Job{}, fmt.Errorf("scan queued job: %w", err)
	}
	job.RunnerPool = core.RunnerPool(runnerPool)
	return job, nil
}

func scanProcessingJob(row scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var runnerPool string
	if err := row.Scan(&processing.Job.ID, &processing.Job.Source.Adapter, &processing.Job.Source.ID,
		&runnerPool, &processing.MemberID); err != nil {
		return core.ProcessingJob{}, fmt.Errorf("scan processing job: %w", err)
	}
	processing.Job.RunnerPool = core.RunnerPool(runnerPool)
	return processing, nil
}

func scanProcessingJobs(rows Rows) (jobs []core.ProcessingJob, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		job, err := scanProcessingJob(rows)
		if err != nil {
			return nil, err
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
