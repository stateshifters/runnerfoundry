package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestUnreconciledLifecycleClassificationCoversInspectionFailures(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	tests := map[string]struct {
		identity   scaler.RunnerIdentity
		rowResults []fakeRow
	}{
		"invalid identity": {identity: scaler.RunnerIdentity{}},
		"authority":        {identity: identity, rowResults: []fakeRow{{err: errors.New("authority")}}},
		"mirror": {
			identity: identity,
			rowResults: []fakeRow{
				transitionAuthorityRow("node", scaler.RunnerProviderLibvirt, identity.RunnerName),
				{err: errors.New("mirror")},
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			transaction := (&fakeDatabaseTransaction{rowResults: test.rowResults}).databaseTransaction()
			if err := classifyUnreconciledClusterLibvirtLifecycle(
				ctx, transaction, "linux", "repo", test.identity,
			); err == nil {
				t.Fatal("expected lifecycle classification failure")
			}
		})
	}
}

func transitionAuthorityRow(
	nodeID NodeID,
	provider scaler.RunnerProvider,
	runnerName string,
) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], nodeID)
		setFakeDestination(destinations[1], provider)
		setFakeDestination(destinations[2], runnerName)
	}}
}

func TestLifecycleTransitionConflictRejectsDuplicateMirrorRows(t *testing.T) {
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	err := classifyClusterLifecycleTransitionConflict(
		"linux", "repo", identity,
		clusterLifecycleTransitionAuthority{provider: scaler.RunnerProviderLibvirt}, 2,
	)
	if !errors.Is(err, scaler.ErrRunnerIdentityInvariant) {
		t.Fatalf("duplicate mirror error = %v", err)
	}
}

func TestUnchangedMirrorClassificationCoversIdentityStates(t *testing.T) {
	ctx := context.Background()
	identity := scaler.RunnerIdentity{RequestID: 42, RunnerName: "runner-42"}
	tests := map[string]struct {
		counts []int64
		want   error
	}{
		"absent":   {counts: []int64{0, 0}},
		"exact":    {counts: []int64{1, 1, 1}},
		"mismatch": {counts: []int64{1, 1, 0}, want: scaler.ErrRunnerIdentityMismatch},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rows := make([]fakeRow, 0, len(test.counts))
			for _, count := range test.counts {
				rows = append(rows, integerRow(count))
			}
			err := classifyUnchangedClusterLibvirtIdentity(
				ctx, (&fakeDatabaseTransaction{rowResults: rows}).databaseTransaction(),
				"coverage-scale", "coverage-repository", identity,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("classification error = %v, want %v", err, test.want)
			}
		})
	}
}
