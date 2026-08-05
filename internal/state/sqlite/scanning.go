package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

func scanChangedJob(row scanner, operation string) (core.JobRecord, error) {
	record, err := scanJobRow(row)
	if err != nil {
		return core.JobRecord{}, fmt.Errorf("%s: %w", operation, err)
	}
	return record, nil
}

func scanJobRows(rows Rows) (records []core.JobRecord, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		record, err := scanJobRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job rows: %w", err)
	}
	return records, nil
}

func scanJobRow(row scanner) (core.JobRecord, error) {
	var record core.JobRecord
	var jobID string
	var enqueuedAt int64
	var runnerPool, state string
	var memberID, resultOutcome, resultOrigin, resultReason sql.NullString
	var claimedAt, startedAt, acquisitionStartedAt, acquiredAt sql.NullInt64
	if err := row.Scan(&jobID, &record.Job.Source.Adapter, &record.Job.Source.ID, &runnerPool, &state,
		&enqueuedAt, &memberID, &claimedAt, &startedAt, &acquisitionStartedAt, &acquiredAt,
		&resultOutcome, &resultOrigin, &resultReason); err != nil {
		return core.JobRecord{}, fmt.Errorf("scan job columns: %w", err)
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		return core.JobRecord{}, fmt.Errorf("parse job UUID: %w", err)
	}
	record.Job.ID, record.Job.RunnerPool = parsedJobID, core.RunnerPool(runnerPool)
	record.Job.EnqueuedAt, record.State = time.Unix(0, enqueuedAt).UTC(), core.JobState(state)
	if memberID.Valid {
		parsedMemberID, err := uuid.Parse(memberID.String)
		if err != nil {
			return core.JobRecord{}, fmt.Errorf("parse attempting member UUID: %w", err)
		}
		record.AttemptingMember = parsedMemberID
	}
	applySQLiteOptionalFields(&record, claimedAt, startedAt, acquisitionStartedAt, acquiredAt,
		resultOutcome, resultOrigin, resultReason)
	return record, nil
}

func applySQLiteOptionalFields(record *core.JobRecord, claimedAt, startedAt, acquisitionStartedAt, acquiredAt sql.NullInt64,
	resultOutcome, resultOrigin, resultReason sql.NullString) {
	if claimedAt.Valid {
		record.ClaimedAt = time.Unix(0, claimedAt.Int64).UTC()
	}
	if startedAt.Valid {
		record.StartedAt = time.Unix(0, startedAt.Int64).UTC()
	}
	if acquisitionStartedAt.Valid {
		record.AcquisitionStartedAt = time.Unix(0, acquisitionStartedAt.Int64).UTC()
	}
	if acquiredAt.Valid {
		record.AcquiredAt = time.Unix(0, acquiredAt.Int64).UTC()
	}
	if resultOutcome.Valid {
		record.Result = &core.Result{Outcome: core.Outcome(resultOutcome.String),
			Origin: core.ResultOrigin(resultOrigin.String), Reason: resultReason.String}
	}
}

func scanReceiptRow(row scanner) (core.Receipt, error) {
	var receipt core.Receipt
	var outcome, origin, delivery string
	if err := row.Scan(&receipt.Source.Adapter, &receipt.Source.ID, &outcome, &origin, &delivery); err != nil {
		return core.Receipt{}, fmt.Errorf("scan receipt columns: %w", err)
	}
	receipt.Outcome, receipt.Origin = core.Outcome(outcome), core.ResultOrigin(origin)
	receipt.Delivery = core.ResultDelivery(delivery)
	return receipt, nil
}

func rollback(ctx context.Context, transaction Transaction, open *bool) {
	if *open {
		if err := transaction.Rollback(context.WithoutCancel(ctx)); err != nil {
			return
		}
	}
}
