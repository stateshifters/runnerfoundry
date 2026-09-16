package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func scanJob(row Scanner) (core.Job, error) {
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

func scanProcessingJob(row Scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var runnerAccepted bool
	var jobID, sourceAdapter, sourceIdentifier, runnerPool, memberID, runnerName, provisioningFailure string
	if err := row.Scan(&jobID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &memberID, &runnerName, &runnerAccepted, &processing.SourceTerminal,
		&provisioningFailure); err != nil {
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
	processing.RunnerAccepted = runnerAccepted
	processing.ProvisioningFailure = core.ProvisioningFailureState(provisioningFailure)
	return processing, nil
}

func scanClaimedProcessingJob(row Scanner) (core.ProcessingJob, error) {
	var processing core.ProcessingJob
	var runnerAccepted bool
	var jobID, sourceAdapter, sourceIdentifier, runnerPool, memberID, runnerName, endpointID, provisioningFailure string
	var claimedAt int64
	if err := row.Scan(&jobID, &sourceAdapter, &sourceIdentifier,
		&runnerPool, &memberID, &runnerName, &claimedAt, &endpointID, &runnerAccepted,
		&processing.SourceTerminal, &provisioningFailure); err != nil {
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
	processing.RunnerAccepted = runnerAccepted
	processing.ProvisioningFailure = core.ProvisioningFailureState(provisioningFailure)
	return processing, nil
}

func readJob(ctx context.Context, transaction Transaction, statement string, arguments ...any) (core.Job, error) {
	var result core.Job
	err := transaction.ReadRow(ctx, statement, func(row Scanner) error {
		var err error
		result, err = scanJob(row)
		return err
	}, arguments...)
	if err != nil {
		return core.Job{}, fmt.Errorf("read persisted job: %w", err)
	}
	return result, nil
}

func readProcessingJob(ctx context.Context, transaction Transaction, statement string, arguments ...any) (core.ProcessingJob, error) {
	var result core.ProcessingJob
	err := transaction.ReadRow(ctx, statement, func(row Scanner) error {
		var err error
		result, err = scanProcessingJob(row)
		return err
	}, arguments...)
	if err != nil {
		return core.ProcessingJob{}, fmt.Errorf("read persisted job: %w", err)
	}
	return result, nil
}

func readClaimedProcessingJob(ctx context.Context, transaction Transaction, statement string, arguments ...any) (core.ProcessingJob, error) {
	var result core.ProcessingJob
	err := transaction.ReadRow(ctx, statement, func(row Scanner) error {
		var err error
		result, err = scanClaimedProcessingJob(row)
		return err
	}, arguments...)
	if err != nil {
		return core.ProcessingJob{}, fmt.Errorf("read persisted job: %w", err)
	}
	return result, nil
}

func readClaimedProcessingJobs(ctx context.Context, transaction Transaction, statement string, arguments ...any) ([]core.ProcessingJob, error) {
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
