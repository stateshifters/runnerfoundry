//go:build integration

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterExpiredWaitingRunnerRetainsDemandForFreshAssignment(t *testing.T) {
	for name, provider := range map[string]scaler.RunnerProvider{
		"docker": scaler.RunnerProviderDocker, "libvirt": scaler.RunnerProviderLibvirt,
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newAssignmentTransitionFixture(t)
			claimed := fixture.claimForStart(t)
			ctx := context.Background()
			startedAt := fixture.now.Add(time.Minute)
			runner := scaler.RunnerLifecycle{
				NodeID: string(claimed.NodeID), ScaleSet: claimed.ScaleSet, Repository: claimed.Repository,
				Provider: provider, RunnerName: "linux-1", RequestID: claimed.RequestID,
				AssignmentID: int64(claimed.ID), InstallationID: int64(claimed.InstallationID),
				GitHubScaleSetID: claimed.GitHubScaleSetID, GitHubRunnerID: 101,
				Phase: scaler.RunnerProvisioning, CreatedAt: startedAt,
			}
			if err := fixture.store.RegisterRunner(ctx, runner); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.RegisterRunner(ctx, runner); err != nil {
				t.Fatalf("matching duplicate: %v", err)
			}
			conflict := runner
			conflict.RequestID++
			if err := fixture.store.RegisterRunner(ctx, conflict); err == nil {
				t.Fatal("expected conflicting duplicate")
			}
			if err := fixture.store.ConfirmRunnerWaiting(ctx, runner, startedAt); err != nil {
				t.Fatal(err)
			}
			lifecycles, err := fixture.store.RunnerLifecycles(ctx, claimed.ScaleSet, "")
			if err != nil || len(lifecycles) != 1 {
				t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
			}
			if changed, excessErr := fixture.store.RequestExcessWaitingRunnerReaps(ctx, claimed.ScaleSet, claimed.Repository, 0, "decreased", startedAt); excessErr != nil || changed != 0 {
				t.Fatalf("cluster excess changed=%d err=%v", changed, excessErr)
			}
			changed, err := fixture.store.RequestExpiredRunnerReaps(
				ctx, claimed.ScaleSet, claimed.Repository, startedAt.Add(10*time.Minute), startedAt.Add(11*time.Minute),
			)
			if err != nil || changed != 1 {
				t.Fatalf("expired runners = %d, %v", changed, err)
			}
			pending, err := fixture.store.PendingNodeRunnerReaps(ctx, claimed.NodeID)
			if err != nil || len(pending) != 1 || pending[0].ReapReason != "job_start_timeout" {
				t.Fatalf("pending runner reaps = %#v, %v", pending, err)
			}
			allPending, err := fixture.store.PendingRunnerReaps(ctx, claimed.ScaleSet, "")
			if err != nil || len(allPending) != 1 {
				t.Fatalf("all pending=%#v err=%v", allPending, err)
			}
			if err = fixture.store.ConfirmRunnerReaped(ctx, pending[0]); err != nil {
				t.Fatal(err)
			}
			if err = fixture.store.ConfirmRunnerReaped(ctx, pending[0]); err == nil {
				t.Fatal("expected missing reap confirmation failure")
			}
			replacement, err := fixture.store.AssignOldest(ctx, claimed.ScaleSet, fixture.now, fixture.liveFor)
			if err != nil || replacement.RequestID != claimed.RequestID || replacement.ID == claimed.ID {
				t.Fatalf("replacement assignment = %#v, %v", replacement, err)
			}
		})
	}
}

func TestClusterExactRunnerReapIsMirroredForBothProviders(t *testing.T) {
	for name, provider := range map[string]scaler.RunnerProvider{"docker": scaler.RunnerProviderDocker, "libvirt": scaler.RunnerProviderLibvirt} {
		t.Run(name, func(t *testing.T) {
			fixture := newAssignmentTransitionFixture(t)
			claimed := fixture.claimForStart(t)
			ctx := context.Background()
			runner := scaler.RunnerLifecycle{NodeID: string(claimed.NodeID), ScaleSet: claimed.ScaleSet, Repository: claimed.Repository,
				Provider: provider, RunnerName: "linux-42", RequestID: claimed.RequestID, AssignmentID: int64(claimed.ID),
				InstallationID: int64(claimed.InstallationID), GitHubScaleSetID: claimed.GitHubScaleSetID, CreatedAt: fixture.now}
			if err := fixture.store.RegisterRunner(ctx, runner); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.RequestRunnerReap(ctx, claimed.ScaleSet, claimed.Repository, claimed.RequestID, "", "canceled", fixture.now); err != nil {
				t.Fatal(err)
			}
			pending, err := fixture.store.PendingRunnerReaps(ctx, claimed.ScaleSet, claimed.Repository)
			if err != nil || len(pending) != 1 || pending[0].ReapReason != "canceled" {
				t.Fatalf("pending=%#v err=%v", pending, err)
			}
			nodePending, err := fixture.store.PendingNodeRunnerReaps(ctx, claimed.NodeID)
			if err != nil || len(nodePending) != 1 || !nodePending[0].WaitingSince.IsZero() {
				t.Fatalf("node pending=%#v err=%v", nodePending, err)
			}
			lifecycles, err := fixture.store.RunnerLifecycles(ctx, claimed.ScaleSet, claimed.Repository)
			if err != nil || len(lifecycles) != 1 || !lifecycles[0].WaitingSince.IsZero() {
				t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
			}
		})
	}
}

func TestClusterBusyRunnerDoesNotExpire(t *testing.T) {
	fixture := newAssignmentTransitionFixture(t)
	claimed := fixture.claimForStart(t)
	ctx := context.Background()
	startedAt := fixture.now.Add(time.Minute)
	runner := scaler.RunnerLifecycle{
		NodeID: string(claimed.NodeID), ScaleSet: claimed.ScaleSet, Repository: claimed.Repository,
		Provider: scaler.RunnerProviderLibvirt, RunnerName: "linux-1", RequestID: claimed.RequestID,
		AssignmentID: int64(claimed.ID), InstallationID: int64(claimed.InstallationID),
		GitHubScaleSetID: claimed.GitHubScaleSetID, GitHubRunnerID: 101,
		Phase: scaler.RunnerProvisioning, CreatedAt: startedAt,
	}
	if err := fixture.store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.ConfirmRunnerWaiting(ctx, runner, startedAt); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.MarkRunnerBusy(
		ctx, claimed.ScaleSet, claimed.Repository, claimed.RequestID, runner.RunnerName, 101, startedAt.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	changed, err := fixture.store.RequestExpiredRunnerReaps(
		ctx, claimed.ScaleSet, claimed.Repository, startedAt.Add(time.Hour), startedAt.Add(2*time.Hour),
	)
	if err != nil || changed != 0 {
		t.Fatalf("busy runner expired: changed=%d err=%v", changed, err)
	}
}
