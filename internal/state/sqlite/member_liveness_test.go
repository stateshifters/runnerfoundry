package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestLiveMemberIDsReturnsTheLiveMembershipSnapshot(t *testing.T) {
	t.Parallel()
	transaction := transactionStub{
		query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsFromValues([][]any{{"member-one"}}), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	members, err := newStore(executorStub{begin: transactionBeginning(transaction)}).LiveMemberIDs(
		context.Background(), time.Unix(100, 0))
	if err != nil || len(members) != 1 || members[0] != core.MemberID("member-one") {
		t.Fatalf("live members = %v, %v", members, err)
	}
}
