package daemon

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
)

func TestRunClusteredDaemonPublishesStartupAndShutdown(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{}
	ctx, cancel := context.WithCancel(context.Background())
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.notifier = cancellingNotifier{cancel: cancel}

	if err := runClusteredDaemon(ctx, Options{}, daemonUnderTest, configured); err != nil {
		t.Fatal(err)
	}
	if state.migrations != 1 || len(state.publications) != 2 {
		t.Fatalf("migrations=%d publications=%#v", state.migrations, state.publications)
	}
	if !state.publications[0].healthy || state.publications[1].healthy {
		t.Fatalf("startup and shutdown health was not published: %#v", state.publications)
	}
	if state.publications[0].nodeID != "spock" || state.publications[1].nodeID != "spock" {
		t.Fatalf("operating-system hostname was not used as node identity: %#v", state.publications)
	}
	contribution := state.publications[0].contributions[0]
	if contribution.ScaleSet != "linux" || contribution.MaxRunners != 10 || !contribution.Healthy {
		t.Fatalf("host-local contribution was not published: %#v", contribution)
	}
}

func TestClusteredLifecycleSmoke(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{
		leases:   []cluster.Lease{&fakeClusterLease{alive: true}},
		statuses: []cluster.ScaleSetStatus{{Name: "linux", Compatible: false}},
	}
	reloadRequests := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	providerHealthy := true
	notifier := &clusterLifecycleNotifier{onReady: func() {
		providerHealthy = false
		reloadRequests <- struct{}{}
	}}
	state.onPublish = func() {
		if len(state.publications) == 2 {
			cancel()
		}
	}
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.loadConfig = func(string) (config.Config, error) { return configured, nil }
	daemonUnderTest.notifier = notifier
	daemonUnderTest.providerHealthy = func(context.Context, config.ScaleSetConfig) error {
		if providerHealthy {
			return nil
		}
		return errors.New("provider unavailable")
	}
	if err := runClusteredDaemon(ctx, Options{ReloadRequests: reloadRequests}, daemonUnderTest, configured); err != nil {
		t.Fatal(err)
	}
	if !notifier.ready || state.migrations != 1 || len(state.publications) != 3 {
		t.Fatalf("ready=%t migrations=%d publications=%d", notifier.ready, state.migrations, len(state.publications))
	}
	if !state.publications[0].contributions[0].Healthy || state.publications[1].contributions[0].Healthy {
		t.Fatalf("provider health transition was not published: %#v", state.publications)
	}
	if state.publications[2].healthy {
		t.Fatalf("shutdown did not withdraw the node: %#v", state.publications[2])
	}
}

func TestRunClusteredDaemonReconcilesImmediatelyWhenElected(t *testing.T) {
	configured := clusteredConfig(t)
	scaleSetReads := 0
	ctx, cancel := context.WithCancel(context.Background())
	state := &fakeClusterState{
		leases:      []cluster.Lease{&fakeClusterLease{alive: true}},
		statuses:    []cluster.ScaleSetStatus{{Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: false}},
		onScaleSets: func() { scaleSetReads++; cancel() },
	}
	daemonUnderTest := clusteredDaemonForTest(state)
	ready := false
	daemonUnderTest.notifier = readyRecordingNotifier{ready: &ready}
	if err := runClusteredDaemon(ctx, Options{}, daemonUnderTest, configured); err != nil {
		t.Fatal(err)
	}
	if scaleSetReads != 1 {
		t.Fatalf("startup reconciliation read scale sets %d times", scaleSetReads)
	}
}

func TestLeadershipContenderRetriesThenAcquires(t *testing.T) {
	attempts := 0
	state := &fakeClusterState{waitForLeader: func(context.Context) (cluster.Lease, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("temporary failure")
		}
		return &cluster.LeaderLease{}, nil
	}}
	results := make(chan leadershipResult, 1)
	daemon := clusteredDaemon{waitForLeader: fakeWaitForLeader(state), logger: slog.Default()}
	daemon.contendForLeadership(context.Background(), results, make(chan struct{}))
	if result := <-results; result.lease == nil || attempts != 2 {
		t.Fatalf("result=%#v attempts=%d", result, attempts)
	}
}

func TestCanceledLeadershipContenderStopsWithoutAcquiring(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state := &fakeClusterState{}
	daemon := clusteredDaemon{waitForLeader: fakeWaitForLeader(state), logger: slog.Default()}
	daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), make(chan struct{}))
}

func TestCanceledLeadershipDeliveryClosesLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lease := &fakeClusterLease{closeErr: errors.New("close failed")}
	daemon := clusteredDaemon{logger: slog.Default()}
	daemon.deliverLeadership(ctx, make(chan leadershipResult), lease)
	if !lease.closed {
		t.Fatal("canceled delivery did not close lease")
	}
}

func TestLeadershipContenderShutdownClosesUnclaimedLease(t *testing.T) {
	lease := &fakeClusterLease{closeErr: errors.New("close failed")}
	done := make(chan struct{})
	close(done)
	results := make(chan leadershipResult, 1)
	results <- leadershipResult{lease: lease}
	daemon := clusteredDaemon{
		logger: slog.Default(), leadershipResults: results,
		stopContender: func() {}, contenderDone: done,
	}
	daemon.stopLeadershipContender(context.Background())
	if !lease.closed {
		t.Fatal("unclaimed lease was not closed")
	}
	daemon.stopLeadershipContender(context.Background())
}

func TestStartingLeadershipContenderTwiceOpensOneWaiter(t *testing.T) {
	waiterStarted := make(chan struct{}, 2)
	state := &fakeClusterState{waitForLeader: func(ctx context.Context) (cluster.Lease, error) {
		waiterStarted <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	daemon := clusteredDaemon{waitForLeader: fakeWaitForLeader(state), logger: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	daemon.startContender(ctx)
	<-waiterStarted
	daemon.startContender(ctx)
	select {
	case <-waiterStarted:
		t.Fatal("second contender opened another PostgreSQL waiter")
	default:
	}
	cancel()
	daemon.stopLeadershipContender(context.Background())
}

func TestLeadershipRetryStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitForLeadershipRetry(ctx, time.Nanosecond) {
		t.Fatal("canceled retry waited")
	}
}

func TestLeadershipContenderStopsWhenCanceledDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	state := &fakeClusterState{waitForLeader: func(context.Context) (cluster.Lease, error) {
		return nil, errors.New("temporary failure")
	}}
	logger := slog.New(slog.NewTextHandler(cancelingWriter{cancel: cancel}, nil))
	daemon := clusteredDaemon{waitForLeader: fakeWaitForLeader(state), logger: logger}
	daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), make(chan struct{}))
}

type cancelingWriter struct {
	cancel context.CancelFunc
}

func (writer cancelingWriter) Write(contents []byte) (int, error) {
	writer.cancel()
	return len(contents), nil
}
