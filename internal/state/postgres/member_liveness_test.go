package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestLiveMemberIDsReturnsInclusiveOrderedSnapshot(t *testing.T) {
	t.Parallel()
	protocol := &recordedTransaction{queryRows: rowsFromValues([][]any{{"member-one"}, {"member-two"}})}
	members, err := newStore(protocol.executor()).LiveMemberIDs(context.Background(), fixedTime())
	if err != nil || len(members) != 2 || members[0] != core.MemberID("member-one") ||
		members[1] != core.MemberID("member-two") || protocol.commitCalls != 1 {
		t.Fatalf("members = %#v, commits=%d, error=%v", members, protocol.commitCalls, err)
	}
	if len(protocol.statements) != 1 || !strings.Contains(protocol.statements[0], "heartbeat_at >= $1") ||
		!strings.Contains(protocol.statements[0], "ORDER BY member_id") || len(protocol.arguments[0]) != 1 ||
		protocol.arguments[0][0] != fixedTime() {
		t.Fatalf("protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestLiveMemberIDsReportsEveryTransactionFailure(t *testing.T) {
	t.Parallel()
	assertTransactionalListFailures(t, func(store *Store) error {
		_, err := store.LiveMemberIDs(context.Background(), fixedTime())
		return err
	})
}
