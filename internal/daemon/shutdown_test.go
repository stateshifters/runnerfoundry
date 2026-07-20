package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestLifecycleFailurePathsRemainExplicit(t *testing.T) {
	if _, err := loadConfiguration("config", standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("load failed") },
	}); err == nil {
		t.Fatal("expected load failure")
	}
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return errors.New("shutdown failed") },
	}}}
	if err := removeRepositoryImmediately(context.Background(), &activeConfig, "repo", activeConfig.Repositories["repo"]); err == nil {
		t.Fatal("expected immediate removal failure")
	}
	if err := shutdownWithTimeout(context.Background(), activeConfig, immediateShutdown); err == nil {
		t.Fatal("expected forced shutdown failure")
	}
}

func TestShutdownCleansLifecyclesConcurrentlyWithIndependentContexts(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	activeConfig := testActiveConfiguration()
	for _, repositoryURL := range []string{"repo-a", "repo-b"} {
		activeConfig.Repositories[repositoryURL] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
			StopPolling: func() {},
			Shutdown: func(ctx context.Context) error {
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}}}
	}
	finished := make(chan error, 1)
	go func() { finished <- shutdownWithTimeout(context.Background(), activeConfig, drainShutdown) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected lifecycle cleanup to run concurrently")
		}
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("expected concurrent cleanup success, got %v", err)
	}
}

func TestTerminationReportsCleanupFailuresAsWarnings(t *testing.T) {
	logs := &bytes.Buffer{}
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling: func() {},
		Shutdown:    func(context.Context) error { return errors.New("close failed") },
	})
	activeConfig.Logger = slog.New(slog.NewTextHandler(logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runActiveConfiguration(ctx, Options{}, standaloneDaemon{}, activeConfig, time.Hour); err != nil {
		t.Fatalf("expected termination cleanup failure not to fail the process, got %v", err)
	}
	if !strings.Contains(logs.String(), "shutdown cleanup incomplete") || !strings.Contains(logs.String(), "close failed") {
		t.Fatalf("expected actionable shutdown warning, got %q", logs.String())
	}
}

func TestApplyDiscoverySurfacesRemovalFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{InstallationID: 1, Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return nil },
	}}}
	if err := applyDiscovery(context.Background(), standaloneDaemon{}, &activeConfig, githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}); err == nil {
		t.Fatal("expected removal failure")
	}
}

func TestStartRepositorySurfacesPruneFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Config.Prune = true
	daemonUnderTest := standaloneDaemon{newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")}, nil
	}}
	if _, err := startRepository(context.Background(), daemonUnderTest, &activeConfig, githubapp.Repository{InstallationID: 1, URL: "repo"}); err == nil {
		t.Fatal("expected prune failure")
	}
}

func TestValidationAndPruneFailurePaths(t *testing.T) {
	activeConfig := testActiveConfiguration()
	discovery := githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{
			{InstallationID: 1, URL: "https://github.com/acme/repo.a"},
			{InstallationID: 1, URL: "https://github.com/acme/repo-a"},
		},
		InstallationFailures: map[int64]error{},
	}
	if err := applyDiscovery(context.Background(), standaloneDaemon{}, &activeConfig, discovery); err == nil {
		t.Fatal("expected local name collision")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := pruneRepository(context.Background(), logger, nonPruningScaleSetClient{}, "repo", []config.ScaleSetConfig{{Name: "linux"}}); err == nil {
		t.Fatal("expected unsupported prune client error")
	}
	invalidConfigPath := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalidConfigPath, []byte("github: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(invalidConfigPath); err == nil {
		t.Fatal("expected invalid config error")
	}
}

func TestDrainCancellationAndRollbackFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) { return 1, nil },
	}}}
	if err := drainConfiguration(ctx, activeConfig, time.Hour); err == nil {
		t.Fatal("expected cancelled drain")
	}
	rollbackConfig := testActiveConfiguration()
	rollback := managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return errors.New("shutdown failed") },
	}}}
	if err := rollbackRepositoryStartup(context.Background(), &rollbackConfig, "repo", rollback); err == nil {
		t.Fatal("expected rollback failure")
	}
}

func testActiveConfiguration() activeConfiguration {
	scaleSetConfig := config.ScaleSetConfig{Name: "linux", MaxRunners: 1}
	return activeConfiguration{
		Config:       config.Config{ScaleSets: []config.ScaleSetConfig{scaleSetConfig}},
		Coordinators: map[string]*scaler.Coordinator{"linux": scaler.NewCoordinator(1)},
		Repositories: map[string]managedRepository{},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func inertLifecycle() scaleSetLifecycle {
	return scaleSetLifecycle{
		StopPolling:   func() {},
		Drain:         func(context.Context) (int, error) { return 0, nil },
		DestroyActive: func(context.Context) error { return nil },
		Shutdown:      func(context.Context) error { return nil },
	}
}

type recordingRecoveryWorker struct {
	running   int
	existing  map[int64]bool
	existsErr error
	acquired  []int64
	started   []int64
	assigned  int
}

func (worker *recordingRecoveryWorker) Running(context.Context) (int, error) {
	return worker.running, nil
}

func (worker *recordingRecoveryWorker) Exists(_ context.Context, requestID int64) (bool, error) {
	return worker.existing[requestID], worker.existsErr
}

func (worker *recordingRecoveryWorker) Acquire(_ context.Context, requestID int64) error {
	worker.acquired = append(worker.acquired, requestID)
	return nil
}

func (worker *recordingRecoveryWorker) StartOne(_ context.Context, requestID int64) error {
	worker.started = append(worker.started, requestID)
	return nil
}

func (worker *recordingRecoveryWorker) StartAssigned(context.Context) error {
	worker.assigned++
	return nil
}

func runnableConfig(t *testing.T) config.Config {
	t.Helper()
	privateKeyPath := filepath.Join(t.TempDir(), "app.pem")
	if err := os.WriteFile(privateKeyPath, []byte("private-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.Config{
		GitHub: config.GitHubConfig{
			App:               config.GitHubAppConfig{AppID: 1, PrivateKeyFile: privateKeyPath},
			DiscoveryInterval: "5m",
		},
		Log: config.LogConfig{Format: "text"},
		ScaleSets: []config.ScaleSetConfig{{
			Name: "linux", MaxRunners: 1,
			Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
		}},
	}
}

func activeWithLifecycle(lifecycle scaleSetLifecycle) activeConfiguration {
	if lifecycle.StopPolling == nil {
		lifecycle.StopPolling = func() {}
	}
	if lifecycle.Drain == nil {
		lifecycle.Drain = func(context.Context) (int, error) { return 0, nil }
	}
	if lifecycle.Shutdown == nil {
		lifecycle.Shutdown = func(context.Context) error { return nil }
	}
	active := testActiveConfiguration()
	active.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": lifecycle}}
	return active
}

type staticDiscoverer struct {
	result githubapp.DiscoveryResult
	err    error
}

func (discoverer staticDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	return discoverer.result, discoverer.err
}

type cancellingNotifier struct {
	cancel context.CancelFunc
}

func (notifier cancellingNotifier) Ready(context.Context) error {
	notifier.cancel()
	return nil
}

func (cancellingNotifier) Reloading(context.Context) error { return nil }

type failingNotifier struct {
	readyErr  error
	reloadErr error
}

type readyRecordingNotifier struct {
	ready *bool
}

func (notifier readyRecordingNotifier) Ready(context.Context) error {
	*notifier.ready = true
	return nil
}

func (readyRecordingNotifier) Reloading(context.Context) error { return nil }

type readinessCheckingDiscoverer struct {
	ready  *bool
	cancel context.CancelFunc
}

func (discoverer readinessCheckingDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	if !*discoverer.ready {
		return githubapp.DiscoveryResult{}, errors.New("discovery ran before readiness")
	}
	discoverer.cancel()
	return githubapp.DiscoveryResult{}, errors.New("GitHub unavailable")
}

func (notifier failingNotifier) Ready(context.Context) error     { return notifier.readyErr }
func (notifier failingNotifier) Reloading(context.Context) error { return notifier.reloadErr }

type recordingDiscoverer struct {
	result        githubapp.DiscoveryResult
	calls         int
	afterDiscover func(call int)
}

type reloadCancellingNotifier struct {
	cancel      context.CancelFunc
	readyCalls  int
	reloadCalls int
}

func (notifier *reloadCancellingNotifier) Ready(context.Context) error {
	notifier.readyCalls++
	if notifier.readyCalls == 2 {
		notifier.cancel()
	}
	return nil
}

func (notifier *reloadCancellingNotifier) Reloading(context.Context) error {
	notifier.reloadCalls++
	return nil
}

type nonPruningScaleSetClient struct{}

func assertStructuredAction(t *testing.T, logBytes []byte, action string, phase string) {
	t.Helper()
	for _, line := range bytes.Split(bytes.TrimSpace(logBytes), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode structured log %q: %v", line, err)
		}
		if event["action"] == action && event["phase"] == phase {
			return
		}
	}
	t.Fatalf("expected action %q phase %q in logs %s", action, phase, logBytes)
}

func (nonPruningScaleSetClient) GetRunnerGroupByName(context.Context, string) (*upstream.RunnerGroup, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) GetRunnerScaleSet(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) CreateRunnerScaleSet(context.Context, *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) DeleteRunnerScaleSet(context.Context, int) error { return nil }

func (nonPruningScaleSetClient) MessageSessionClient(context.Context, int, string) (scaleSetMessageClient, error) {
	return scaleSetMessageClient{}, nil
}

func (nonPruningScaleSetClient) GenerateJitRunnerConfig(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) GetRunnerByName(context.Context, string) (*upstream.RunnerReference, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) RemoveRunner(context.Context, int64) error { return nil }

func (discoverer *recordingDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	discoverer.calls++
	discoverer.afterDiscover(discoverer.calls)
	return discoverer.result, nil
}
