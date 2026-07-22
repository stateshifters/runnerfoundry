package daemon

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/postgresconfig"
)

func TestStandaloneRepositoryStartupRejectsAnInvalidRepositoryURL(t *testing.T) {
	configured := runnableConfig(t)
	active := testActiveConfiguration()
	active.Config = configured
	active.Credentials = testAppCredentials(t)
	active.Logger = slog.Default()

	_, err := startRepository(context.Background(), newStandaloneDaemon(), &active, githubapp.Repository{
		URL: "not a repository URL", InstallationID: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "create GitHub client") || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("repository startup error = %v, want client-boundary URL rejection", err)
	}
}

func TestClusteredGitHubConnectionAcceptsConfiguredCredentials(t *testing.T) {
	configured := clusteredConfig(t)
	setValidGitHubCredentials(t, &configured)
	daemon := newClusteredDaemon()
	if err := daemon.connectGitHub(configured); err != nil {
		t.Fatalf("connect GitHub: %v", err)
	}
	if daemon.discoverer == nil {
		t.Fatal("configured credentials did not create repository discovery")
	}
}

func TestClusteredSessionRejectsAnInvalidRepositoryURL(t *testing.T) {
	configured := clusteredConfig(t)
	setValidGitHubCredentials(t, &configured)
	daemon := newClusteredDaemon()
	if err := daemon.connectGitHub(configured); err != nil {
		t.Fatal(err)
	}
	daemon.config = configured
	daemon.logger = slog.Default()
	leader := newClusterLeader(daemon, &fakeClusterLease{alive: true})

	_, err := leader.startSession(context.Background(), githubapp.Repository{
		URL: "not a repository URL", InstallationID: 1,
	}, cluster.ScaleSetStatus{Name: "linux", RunnerGroup: "default", EffectiveCapacity: 1, Compatible: true})
	if err == nil {
		t.Fatal("expected invalid repository URL rejection")
	}
}

func setValidGitHubCredentials(t *testing.T, configured *config.Config) {
	t.Helper()
	credentials := testAppCredentials(t)
	configured.GitHub.App.AppID = credentials.AppID
	configured.GitHub.App.PrivateKey = ""
	configured.GitHub.App.PrivateKeyFile = writeFile(t, "github-app.pem", credentials.PrivateKey)
}

func TestProductionLeadershipConnectionsReportCredentialResolutionFailure(t *testing.T) {
	daemon := newClusteredDaemon()
	credentialFixture := newPostgresCredentialFixture()
	connection, err := daemon.openStore(postgresconfig.Settings{
		Role: "github_scaler", Password: credentialFixture.missingReference,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.tryLeader(context.Background()); err == nil {
		t.Fatal("expected immediate leadership connection failure")
	}
	if _, err = connection.waitForLeader(context.Background()); err == nil {
		t.Fatal("expected blocking leadership connection failure")
	}
	if errors.Is(err, cluster.ErrNotLeader) {
		t.Fatalf("credential failure was reported as lease contention: %v", err)
	}
}

func TestClusterLeadershipResultRetainsConcreteLeaseCapability(t *testing.T) {
	lease := &fakeClusterLease{alive: true}
	result, err := clusterLeadershipResult("try cluster leadership", lease, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.lease != lease || !result.lease.Alive(context.Background()) {
		t.Fatalf("leadership result lease = %#v, want injected lease", result.lease)
	}
}
