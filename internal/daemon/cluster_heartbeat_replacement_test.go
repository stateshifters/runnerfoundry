package daemon

import (
	"context"
	"log/slog"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestClusterHeartbeatIgnoresLeadershipAlreadyReplaced(t *testing.T) {
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), logger: slog.Default(), nodes: state,
		providerHealthy: func(context.Context, config.ScaleSetConfig) error { return nil },
		waitForLeader:   fakeWaitForLeader(state),
	}
	replacement := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
	lease := &fakeClusterLease{alive: false}
	lease.onAlive = func() { daemon.leader = replacement }
	daemon.leader = newClusterLeader(context.Background(), &daemon, lease)
	daemon.heartbeat(context.Background())
	daemon.stopLeadershipContender(context.Background())
	if daemon.currentLeader() != replacement {
		t.Fatalf("current leader = %#v, want replacement", daemon.currentLeader())
	}
}
