package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

func scanChangedJob(row Row, operation string) (core.JobRecord, error) {
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
	var jobID uuid.UUID
	var runnerPool, state string
	var memberID uuid.NullUUID
	var resultOutcome, resultOrigin, resultReason sql.NullString
	var claimedAt, startedAt, acquisitionStartedAt, acquiredAt sql.NullTime
	if err := row.Scan(&jobID, &record.Job.Source.Adapter, &record.Job.Source.ID, &runnerPool, &state,
		&record.Job.EnqueuedAt, &memberID, &claimedAt, &startedAt, &acquisitionStartedAt, &acquiredAt,
		&resultOutcome, &resultOrigin, &resultReason); err != nil {
		return core.JobRecord{}, fmt.Errorf("scan job columns: %w", err)
	}
	record.Job.ID, record.Job.RunnerPool = jobID, core.RunnerPool(runnerPool)
	record.State, record.AttemptingMember = core.JobState(state), memberID.UUID
	applyPostgreSQLOptionalFields(&record, claimedAt, startedAt, acquisitionStartedAt, acquiredAt,
		resultOutcome, resultOrigin, resultReason)
	return record, nil
}

func applyPostgreSQLOptionalFields(record *core.JobRecord, claimedAt, startedAt, acquisitionStartedAt, acquiredAt sql.NullTime,
	resultOutcome, resultOrigin, resultReason sql.NullString) {
	if claimedAt.Valid {
		record.ClaimedAt = claimedAt.Time
	}
	if startedAt.Valid {
		record.StartedAt = startedAt.Time
	}
	if acquisitionStartedAt.Valid {
		record.AcquisitionStartedAt = acquisitionStartedAt.Time
	}
	if acquiredAt.Valid {
		record.AcquiredAt = acquiredAt.Time
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
