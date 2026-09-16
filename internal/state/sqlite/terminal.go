package sqlite

import (
	"context"
	"fmt"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TerminalRemovalResult, error) {
	outcome := core.TerminalRemoved
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		rows, err := transaction.Execute(ctx, deleteTerminalProcessingSQL, jobID.String())
		if err != nil {
			return fmt.Errorf("delete terminal processing: %w", err)
		}
		if rows == 0 {
			outcome, err = classifyTerminalRemoval(ctx, transaction, jobID.String())
		}
		return err
	})
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("remove terminal processing: %w", err)
	}
	return core.TerminalRemovalResult{Outcome: outcome}, nil
}

func classifyTerminalRemoval(ctx context.Context, transaction Transaction, jobID string) (core.TerminalRemovalOutcome, error) {
	var exists bool
	if err := transaction.ReadRow(ctx, processingExistsSQL, func(row Scanner) error { return row.Scan(&exists) }, jobID); err != nil {
		return "", fmt.Errorf("classify terminal processing job: %w", err)
	}
	if exists {
		return core.TerminalNotTerminal, nil
	}
	return core.TerminalMissing, nil
}
