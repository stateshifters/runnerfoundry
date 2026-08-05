package core

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid job state transition")
)

type MemoryJobStore struct {
	mutex        sync.Mutex
	records      map[JobID]JobRecord
	jobsBySource map[SourceReference]JobID
	receipts     map[SourceReference]Receipt
	enqueueOrder []JobID
}

func (repository *MemoryJobStore) MarkStartedBySource(
	_ context.Context,
	source SourceReference,
	startedAt time.Time,
) (JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	jobID, exists := repository.jobsBySource[source]
	if !exists {
		return JobRecord{}, ErrJobNotFound
	}
	record := repository.records[jobID]
	if record.State == JobStateResultPending {
		return record, nil
	}
	if record.State != JobStateAttempting {
		return JobRecord{}, ErrInvalidTransition
	}

	record.StartedAt = startedAt
	repository.records[jobID] = record
	return record, nil
}

func (repository *MemoryJobStore) BeginAcquisition(
	_ context.Context,
	jobID JobID,
	at time.Time,
) (JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	record, exists := repository.records[jobID]
	if !exists {
		return JobRecord{}, ErrJobNotFound
	}
	if record.State != JobStateAttempting {
		return JobRecord{}, ErrInvalidTransition
	}
	if !record.AcquisitionStartedAt.IsZero() {
		return record, nil
	}
	record.AcquisitionStartedAt = at
	repository.records[jobID] = record
	return record, nil
}

func (repository *MemoryJobStore) ConfirmAcquisition(
	_ context.Context,
	jobID JobID,
	at time.Time,
) (JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	record, exists := repository.records[jobID]
	if !exists {
		return JobRecord{}, ErrJobNotFound
	}
	if record.State != JobStateAttempting || record.AcquisitionStartedAt.IsZero() {
		return JobRecord{}, ErrInvalidTransition
	}
	if !record.AcquiredAt.IsZero() {
		return record, nil
	}
	record.AcquiredAt = at
	repository.records[jobID] = record
	return record, nil
}

func (repository *MemoryJobStore) SetResultPending(
	_ context.Context,
	jobID JobID,
	result Result,
) (JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	return repository.transitionToResultPending(jobID, result)
}

func (repository *MemoryJobStore) SetResultPendingBySource(
	_ context.Context,
	source SourceReference,
	result Result,
) (JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	jobID, exists := repository.jobsBySource[source]
	if !exists {
		return JobRecord{}, ErrJobNotFound
	}
	return repository.transitionToResultPending(jobID, result)
}

func (repository *MemoryJobStore) SetMemberAttemptsResultPending(
	_ context.Context,
	member MemberID,
	result Result,
) ([]JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	pending := make([]JobRecord, 0)
	for _, jobID := range repository.enqueueOrder {
		record, exists := repository.records[jobID]
		if !exists || record.State != JobStateAttempting || record.AttemptingMember != member {
			continue
		}
		updated := record
		updated.State = JobStateResultPending
		updated.Result = &result
		repository.records[jobID] = updated
		pending = append(pending, updated)
	}
	return pending, nil
}

func (repository *MemoryJobStore) SetUnstartedAttemptsResultPending(
	_ context.Context,
	runnerPool RunnerPool,
	before time.Time,
	result Result,
) ([]JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	pending := make([]JobRecord, 0)
	for _, jobID := range repository.enqueueOrder {
		record, exists := repository.records[jobID]
		if !exists || record.Job.RunnerPool != runnerPool || record.State != JobStateAttempting ||
			!record.StartedAt.IsZero() || record.ClaimedAt.After(before) {
			continue
		}
		updated := record
		updated.State = JobStateResultPending
		updated.Result = &result
		repository.records[jobID] = updated
		pending = append(pending, updated)
	}
	return pending, nil
}

func (repository *MemoryJobStore) transitionToResultPending(jobID JobID, result Result) (JobRecord, error) {
	record, exists := repository.records[jobID]
	if !exists {
		return JobRecord{}, ErrJobNotFound
	}
	if record.Result != nil && *record.Result != result {
		return JobRecord{}, ErrConflictingResult
	}

	record.State = JobStateResultPending
	record.Result = &result
	repository.records[jobID] = record

	return record, nil
}

func (repository *MemoryJobStore) Lookup(
	_ context.Context,
	jobID JobID,
) (JobRecord, bool, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	record, exists := repository.records[jobID]
	return record, exists, nil
}

func (repository *MemoryJobStore) LookupBySource(
	_ context.Context,
	source SourceReference,
) (JobRecord, bool, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	jobID, exists := repository.jobsBySource[source]
	if !exists {
		return JobRecord{}, false, nil
	}
	return repository.records[jobID], true, nil
}

func (repository *MemoryJobStore) AttemptsByMember(
	_ context.Context,
	member MemberID,
	runnerPool RunnerPool,
) ([]JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	attempts := make([]JobRecord, 0)
	for _, jobID := range repository.enqueueOrder {
		record, exists := repository.records[jobID]
		if exists && record.State == JobStateAttempting &&
			record.AttemptingMember == member && record.Job.RunnerPool == runnerPool {
			attempts = append(attempts, record)
		}
	}
	return attempts, nil
}

func (repository *MemoryJobStore) PendingResults(_ context.Context, limit int) ([]JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	pending := make([]JobRecord, 0)
	for _, jobID := range repository.enqueueOrder {
		record, exists := repository.records[jobID]
		if exists && record.State == JobStateResultPending {
			pending = append(pending, record)
		}
	}
	if limit <= 0 {
		return []JobRecord{}, nil
	}
	if len(pending) > limit {
		pending = pending[:limit]
	}
	return pending, nil
}

func (repository *MemoryJobStore) PendingAcquisitions(_ context.Context, limit int) ([]JobRecord, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	pending := make([]JobRecord, 0)
	for _, jobID := range repository.enqueueOrder {
		record, exists := repository.records[jobID]
		if exists && record.State == JobStateAttempting &&
			!record.AcquisitionStartedAt.IsZero() && record.AcquiredAt.IsZero() {
			pending = append(pending, record)
		}
	}
	if limit <= 0 {
		return []JobRecord{}, nil
	}
	if len(pending) > limit {
		pending = pending[:limit]
	}
	return pending, nil
}

func (repository *MemoryJobStore) ClaimOldest(
	_ context.Context,
	runnerPool RunnerPool,
	member MemberID,
	claimedAt time.Time,
) (JobRecord, bool, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	for _, jobID := range repository.enqueueOrder {
		record, exists := repository.records[jobID]
		if !exists || record.State != JobStateQueued || record.Job.RunnerPool != runnerPool {
			continue
		}
		record.State = JobStateAttempting
		record.AttemptingMember = member
		record.ClaimedAt = claimedAt
		repository.records[record.Job.ID] = record
		return record, true, nil
	}
	return JobRecord{}, false, nil
}

func NewMemoryJobStore() *MemoryJobStore {
	return &MemoryJobStore{
		records:      make(map[JobID]JobRecord),
		jobsBySource: make(map[SourceReference]JobID),
		receipts:     make(map[SourceReference]Receipt),
	}
}

func (repository *MemoryJobStore) Finalize(
	_ context.Context,
	jobID JobID,
	delivery ResultDelivery,
) (Receipt, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	record, exists := repository.records[jobID]
	if !exists || record.Result == nil {
		return Receipt{}, ErrJobNotFound
	}

	receipt := Receipt{
		Source:   record.Job.Source,
		Outcome:  record.Result.Outcome,
		Origin:   record.Result.Origin,
		Delivery: delivery,
	}
	repository.receipts[record.Job.Source] = receipt
	delete(repository.records, jobID)
	delete(repository.jobsBySource, record.Job.Source)

	return receipt, nil
}

func (repository *MemoryJobStore) LookupReceipt(
	_ context.Context,
	source SourceReference,
) (Receipt, bool, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	receipt, exists := repository.receipts[source]
	return receipt, exists, nil
}

func (repository *MemoryJobStore) Enqueue(_ context.Context, request JobRequest) (JobRecord, bool, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()

	if existingID, exists := repository.jobsBySource[request.Source]; exists {
		return repository.records[existingID], false, nil
	}
	if _, finalized := repository.receipts[request.Source]; finalized {
		return JobRecord{}, false, nil
	}

	jobID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(request.Source.Adapter+"\x00"+request.Source.ID))
	job := Job{
		ID:         jobID,
		Source:     request.Source,
		RunnerPool: request.RunnerPool,
		EnqueuedAt: request.EnqueuedAt,
	}
	record := JobRecord{Job: job, State: JobStateQueued}
	repository.records[job.ID] = record
	repository.jobsBySource[job.Source] = job.ID
	repository.enqueueOrder = append(repository.enqueueOrder, job.ID)

	return record, true, nil
}
