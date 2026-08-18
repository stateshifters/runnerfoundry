//go:build e2e

package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	"github.com/meza/RunnerFoundry/internal/state/sqlite"
)

func (application *e2eApplication) buildProviderSet(
	ctx context.Context,
	_ config.Config,
	_ core.MemberID,
	_ productionDependencies,
) (map[core.RunnerPool]execution.ExecutionProvider, execution.AggregateRunnerInventory, error) {
	providers := application.nextProviderBuild()
	if err := validateE2EApplicationProviders(ctx, providers); err != nil {
		return nil, execution.AggregateRunnerInventory{}, err
	}
	inventory := execution.NewAggregateRunnerInventory(providers.linux, providers.windows)
	return map[core.RunnerPool]execution.ExecutionProvider{
		"linux": providers.linux, "windows": providers.windows,
	}, inventory, nil
}

type e2eApplicationProviderBuild struct {
	linux   execution.ExecutionProvider
	windows execution.ExecutionProvider
}

func (application *e2eApplication) nextProviderBuild() e2eApplicationProviderBuild {
	application.mutex.Lock()
	defer application.mutex.Unlock()
	provider := application.provider
	windows := application.windows
	if application.providerBuilds > 0 && application.replacementProvider != nil {
		provider = application.replacementProvider
		windows = application.replacementWindows
	}
	buildNumber := application.providerBuilds
	validationErr := application.replacementProviderValidationErr
	application.providerBuilds++
	linuxBoundary := execution.ExecutionProvider(provider)
	if buildNumber > 0 && validationErr != nil {
		linuxBoundary = e2eApplicationValidationFailureProvider{
			ExecutionProvider: linuxBoundary,
			err:               validationErr,
		}
	}
	return e2eApplicationProviderBuild{linux: linuxBoundary, windows: windows}
}

func validateE2EApplicationProviders(ctx context.Context, providers e2eApplicationProviderBuild) error {
	if err := providers.linux.Validate(ctx); err != nil {
		return fmt.Errorf("validate scripted Linux provider: %w", err)
	}
	if err := providers.windows.Validate(ctx); err != nil {
		return fmt.Errorf("validate scripted Windows provider: %w", err)
	}
	return nil
}

type e2eApplicationValidationFailureProvider struct {
	execution.ExecutionProvider
	err error
}

func (provider e2eApplicationValidationFailureProvider) Validate(ctx context.Context) error {
	if err := provider.ExecutionProvider.Validate(ctx); err != nil {
		return fmt.Errorf("validate delegated application provider: %w", err)
	}
	return provider.err
}

func (application *e2eApplication) assertTimedOutLeaveShutdownState(
	t *testing.T,
	linux execution.StartRequest,
	windows execution.StartRequest,
) {
	t.Helper()
	linuxProcessing, linuxErr := application.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	windowsProcessing, windowsErr := application.runtime.state.store.ProcessingByPool(context.Background(), "windows")
	application.assertLeaveShutdownState(
		t, linux, windows, linuxProcessing, windowsProcessing, linuxErr, windowsErr,
	)
}

func (application *e2eApplication) assertCapacityAtCutoff(
	t *testing.T,
	activeSince time.Time,
	want int,
) {
	t.Helper()
	capacity, err := application.runtime.state.store.AvailableRunnerPoolCapacity(
		context.Background(), "linux", activeSince,
	)
	if err != nil || capacity != want {
		t.Fatalf("application Linux capacity since %s = %d, error = %v, want %d",
			activeSince.Format(time.RFC3339Nano), capacity, err, want)
	}
}

func (application *e2eApplication) assertLeaveShutdownRestartReconciliation(
	t *testing.T,
	linux execution.StartRequest,
	windows execution.StartRequest,
) {
	t.Helper()
	replacement, running := application.startLeaveShutdownReplacement(t)
	linuxProcessing, linuxErr := replacement.runtime.state.store.ProcessingByPool(context.Background(), "linux")
	windowsProcessing, windowsErr := replacement.runtime.state.store.ProcessingByPool(context.Background(), "windows")
	assertRestartReconciliationProcessing(t, "linux", linuxProcessing, linuxErr,
		e2eExpectedRestartProcessing{jobID: linux.JobID})
	assertRestartReconciliationProcessing(t, "windows", windowsProcessing, windowsErr,
		e2eExpectedRestartProcessing{jobID: windows.JobID, runnerStarted: true})
	if linuxAttempts, windowsAttempts := replacement.provider.attemptCount(), replacement.windows.attemptCount(); linuxAttempts != 0 || windowsAttempts != 0 {
		t.Fatalf("restart reconciliation provider attempts: linux=%d windows=%d, want zero",
			linuxAttempts, windowsAttempts)
	}
	running.cancel()
	if err := awaitApplicationResult(t, running.result); err != nil {
		t.Fatalf("stop replacement after timed-out leave shutdown: %v", err)
	}
}

func (application *e2eApplication) startLeaveShutdownReplacement(
	t *testing.T,
) (*e2eApplication, e2eApplicationRun) {
	t.Helper()
	replacement := newE2EApplicationWithState(
		t,
		config.StateConfig{Path: application.databasePath},
		application.memberName,
	)
	for index := range replacement.configured.RunnerPools {
		replacement.configured.RunnerPools[index].MaxRunners = 2
	}
	running := startE2EApplication(t, replacement)
	replacement.awaitReady(t, "replacement readiness after timed-out leave shutdown")
	awaitConfiguredBuild(t, replacement.builtConfigs)
	replacement.tickers.awaitInitialGeneration(t)
	return replacement, running
}

func assertRestartReconciliationProcessing(
	t *testing.T,
	runnerPool core.RunnerPool,
	processing []core.ProcessingJob,
	err error,
	want e2eExpectedRestartProcessing,
) {
	t.Helper()
	if err != nil || len(processing) != 1 {
		t.Fatalf("restart reconciliation %s processing = %+v, error = %v", runnerPool, processing, err)
	}
	if processing[0].Job.ID != want.jobID || processing[0].RunnerStarted != want.runnerStarted {
		t.Fatalf("restart reconciliation %s processing = %+v, want job %s started=%t",
			runnerPool, processing[0], want.jobID, want.runnerStarted)
	}
}

type e2eExpectedRestartProcessing struct {
	jobID         core.JobID
	runnerStarted bool
}

type e2eApplicationNotifier struct {
	mutex     sync.Mutex
	ready     chan struct{}
	reloading chan struct{}
	statuses  chan operationalStatus
	recorded  []string
	systemd   *systemdNotifier
}

func newE2EApplicationNotifier() *e2eApplicationNotifier {
	notifier := &e2eApplicationNotifier{
		ready: make(chan struct{}, 8), reloading: make(chan struct{}, 8), statuses: make(chan operationalStatus, 64),
		systemd: newSystemdNotifier(),
	}
	notifier.systemd.notify = func(_ bool, state string) (bool, error) {
		notifier.mutex.Lock()
		notifier.recorded = append(notifier.recorded, state)
		notifier.mutex.Unlock()
		return true, nil
	}
	return notifier
}

func (notifier *e2eApplicationNotifier) snapshot() []string {
	notifier.mutex.Lock()
	defer notifier.mutex.Unlock()
	return append([]string(nil), notifier.recorded...)
}

func (notifier *e2eApplicationNotifier) Ready(ctx context.Context) error {
	if err := notifier.systemd.Ready(ctx); err != nil {
		return fmt.Errorf("render ready notification: %w", err)
	}
	notifier.ready <- struct{}{}
	return nil
}

func (notifier *e2eApplicationNotifier) Reloading(ctx context.Context) error {
	if err := notifier.systemd.Reloading(ctx); err != nil {
		return fmt.Errorf("render reloading notification: %w", err)
	}
	notifier.reloading <- struct{}{}
	return nil
}

func (notifier *e2eApplicationNotifier) Status(ctx context.Context, status operationalStatus) error {
	if err := notifier.systemd.Status(ctx, status); err != nil {
		return fmt.Errorf("render status notification: %w", err)
	}
	notifier.statuses <- status
	return nil
}

type e2eApplicationLogDestination struct {
	mutex   sync.Mutex
	content bytes.Buffer
	changed chan struct{}
}

func newE2EApplicationLogDestination() *e2eApplicationLogDestination {
	return &e2eApplicationLogDestination{changed: make(chan struct{}, 1)}
}

func (destination *e2eApplicationLogDestination) Open(config.LogConfig) (io.WriteCloser, error) {
	return e2eApplicationLogWriter{destination: destination}, nil
}

func (destination *e2eApplicationLogDestination) Write(content []byte) (int, error) {
	destination.mutex.Lock()
	_, _ = destination.content.Write(content)
	destination.mutex.Unlock()
	select {
	case destination.changed <- struct{}{}:
	default:
	}
	return len(content), nil
}

type e2eApplicationLogRecord struct {
	Level           string   `json:"level"`
	Message         string   `json:"msg"`
	Type            string   `json:"type"`
	Component       string   `json:"component"`
	Operation       string   `json:"operation"`
	Member          string   `json:"member"`
	Stage           string   `json:"stage"`
	Outcome         string   `json:"outcome"`
	RunnerPool      string   `json:"runner_pool"`
	MessageID       int      `json:"message_id"`
	ErrorCategory   string   `json:"error_category"`
	FailureDuration *float64 `json:"failure_duration"`
}

func (destination *e2eApplicationLogDestination) records(t *testing.T) []e2eApplicationLogRecord {
	t.Helper()
	destination.mutex.Lock()
	contents := destination.content.String()
	destination.mutex.Unlock()
	records := make([]e2eApplicationLogRecord, 0)
	for _, line := range strings.Split(strings.TrimSpace(contents), "\n") {
		if line == "" {
			continue
		}
		var record e2eApplicationLogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode application log record %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func (destination *e2eApplicationLogDestination) awaitRecord(
	t *testing.T,
	description string,
	matches func(e2eApplicationLogRecord) bool,
) e2eApplicationLogRecord {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		for _, record := range destination.records(t) {
			if matches(record) {
				return record
			}
		}
		select {
		case <-destination.changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
			return e2eApplicationLogRecord{}
		}
	}
}

type e2eApplicationLogWriter struct{ destination *e2eApplicationLogDestination }

func (writer e2eApplicationLogWriter) Write(content []byte) (int, error) {
	return writer.destination.Write(content)
}

func (e2eApplicationLogWriter) Close() error { return nil }

type e2eApplicationTickers struct {
	mutex    sync.Mutex
	tickers  []*e2eManualTicker
	barriers map[*e2eManualTicker]*e2eLaneBarrier
}

func newE2EApplicationTickers() *e2eApplicationTickers {
	return &e2eApplicationTickers{barriers: make(map[*e2eManualTicker]*e2eLaneBarrier)}
}

func (tickers *e2eApplicationTickers) nextTicker(time.Duration) *e2eManualTicker {
	ticker := newE2EManualTicker()
	tickers.mutex.Lock()
	tickers.tickers = append(tickers.tickers, ticker)
	tickers.barriers[ticker] = newE2ELaneBarrier()
	tickers.mutex.Unlock()
	return ticker
}

func (tickers *e2eApplicationTickers) observeMaintenanceLaneCompletion(_ string, observed cycleTicker) {
	ticker, validTicker := observed.(*e2eManualTicker)
	if !validTicker {
		return
	}
	tickers.mutex.Lock()
	barrier := tickers.barriers[ticker]
	tickers.mutex.Unlock()
	if barrier != nil {
		barrier.complete()
	}
}

func (tickers *e2eApplicationTickers) latest(t *testing.T, lane string) *e2eManualTicker {
	t.Helper()
	laneIndex := map[string]int{"membership": 0, "source": 1, "execution": 2}[lane]
	tickers.mutex.Lock()
	defer tickers.mutex.Unlock()
	if len(tickers.tickers) < 3 {
		t.Fatalf("production created %d application tickers, want at least 3", len(tickers.tickers))
	}
	generationStart := len(tickers.tickers) - len(tickers.tickers)%3
	if generationStart == len(tickers.tickers) {
		generationStart -= 3
	}
	return tickers.tickers[generationStart+laneIndex]
}

func (tickers *e2eApplicationTickers) awaitInitialGeneration(t *testing.T) {
	t.Helper()
	for _, lane := range []string{"membership", "source", "execution"} {
		ticker := tickers.latest(t, lane)
		tickers.mutex.Lock()
		barrier := tickers.barriers[ticker]
		tickers.mutex.Unlock()
		if barrier == nil {
			t.Fatalf("production %s lane has no completion barrier", lane)
		}
		barrier.waitAfter(t, "initial application "+lane+" lane", 0)
	}
}

func (tickers *e2eApplicationTickers) cycle(t *testing.T, lane string) {
	t.Helper()
	ticker := tickers.latest(t, lane)
	tickers.cycleTicker(t, ticker, "application "+lane+" maintenance")
}

func (tickers *e2eApplicationTickers) cycleTicker(
	t *testing.T,
	ticker *e2eManualTicker,
	description string,
) {
	t.Helper()
	tickers.mutex.Lock()
	barrier := tickers.barriers[ticker]
	tickers.mutex.Unlock()
	before := barrier.count()
	if !ticker.Tick(time.Now()) {
		t.Fatalf("cannot run %s: application ticker is stopped", description)
	}
	barrier.waitAfter(t, description, before)
}

func awaitSignal(t *testing.T, description string, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitConfiguredBuild(t *testing.T, builds <-chan config.Config) config.Config {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case built := <-builds:
		return built
	case <-timer.C:
		t.Fatal("timed out waiting for production generation build")
		return config.Config{}
	}
}

func awaitApplicationResult(t *testing.T, result <-chan error) error {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal("timed out waiting for production application shutdown")
		return nil
	}
}

func awaitApplicationPolicy(t *testing.T, policies <-chan ActiveRunnerPolicy) ActiveRunnerPolicy {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case policy := <-policies:
		return policy
	case <-timer.C:
		t.Fatal("timed out waiting for application supervisor policy")
		return ""
	}
}

func awaitShutdownTimerDuration(t *testing.T, durations <-chan time.Duration) time.Duration {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case duration := <-durations:
		return duration
	case <-timer.C:
		t.Fatal("timed out waiting for production shutdown timer")
		return 0
	}
}

func assertApplicationCapacityWithdrawn(t *testing.T, databasePath string) {
	t.Helper()
	store, err := sqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("reopen application SQLite state: %v", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close application SQLite state: %v", closeErr)
		}
	}()
	capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", time.Now().Add(-runtimeMemberTTL))
	if err != nil || capacity != 0 {
		t.Fatalf("application capacity after graceful shutdown = %d, error = %v, want 0", capacity, err)
	}
}
