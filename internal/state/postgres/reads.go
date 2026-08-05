package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func (store *Store) Lookup(ctx context.Context, jobID core.JobID) (core.JobRecord, bool, error) {
	return store.lookupJob(ctx, lookupJobSQL, jobID)
}

func (store *Store) LookupBySource(ctx context.Context, source core.SourceReference) (core.JobRecord, bool, error) {
	return store.lookupJob(ctx, lookupJobBySourceSQL, source.Adapter, source.ID)
}

func (store *Store) lookupJob(ctx context.Context, statement string, arguments ...any) (core.JobRecord, bool, error) {
	record, err := scanJobRow(store.executor.QueryRow(ctx, statement, arguments...))
	if errors.Is(err, sql.ErrNoRows) {
		return core.JobRecord{}, false, nil
	}
	if err != nil {
		return core.JobRecord{}, false, fmt.Errorf("lookup job: %w", err)
	}
	return record, true, nil
}

func (store *Store) AttemptsByMember(ctx context.Context, memberID core.MemberID,
	runnerPool core.RunnerPool) ([]core.JobRecord, error) {
	return store.queryJobRecords(ctx, "member attempts", attemptsByMemberSQL, memberID, string(runnerPool))
}

func (store *Store) PendingResults(ctx context.Context, limit int) ([]core.JobRecord, error) {
	return store.queryJobRecords(ctx, "pending results", pendingResultsSQL, limit)
}

func (store *Store) PendingAcquisitions(ctx context.Context, limit int) ([]core.JobRecord, error) {
	return store.queryJobRecords(ctx, "pending acquisitions", pendingAcquisitionsSQL, limit)
}

func (store *Store) LookupReceipt(ctx context.Context, source core.SourceReference) (core.Receipt, bool, error) {
	receipt, err := scanReceiptRow(store.executor.QueryRow(ctx, lookupReceiptSQL, source.Adapter, source.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return core.Receipt{}, false, nil
	}
	if err != nil {
		return core.Receipt{}, false, fmt.Errorf("lookup source receipt: %w", err)
	}
	return receipt, true, nil
}

func (store *Store) queryJobRecords(ctx context.Context, description, statement string,
	arguments ...any) ([]core.JobRecord, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin %s lookup: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	rows, err := transaction.Query(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", description, err)
	}
	records, err := scanJobRows(rows)
	if err != nil {
		return nil, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit %s lookup: %w", description, err)
	}
	return records, nil
}
