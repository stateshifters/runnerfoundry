package sqlite

import (
	"context"
	"fmt"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"time"
)

func (store *Store) LiveMemberIDs(ctx context.Context, cutoff time.Time) ([]core.MemberID, error) {
	var members []core.MemberID
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		return transaction.VisitRows(ctx, "SELECT member_id FROM runnerfoundry_members WHERE heartbeat_at >= ? ORDER BY member_id",
			func(row Scanner) error {
				var member string
				if err := row.Scan(&member); err != nil {
					return fmt.Errorf("scan live member: %w", err)
				}
				members = append(members, core.MemberID(member))
				return nil
			}, cutoff.UnixNano())
	})
	if err != nil {
		return nil, fmt.Errorf("load live members: %w", err)
	}
	return members, nil
}
