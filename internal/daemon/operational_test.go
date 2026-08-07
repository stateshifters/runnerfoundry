package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func closeTestLogging(t *testing.T, logging *loggingManager) {
	t.Helper()
	if err := logging.Close(); err != nil {
		t.Errorf("close test logging: %v", err)
	}
}

func newDebugRuntimeReporter(t *testing.T, destination *memoryLogDestination) *productionRuntimeReporter {
	t.Helper()
	logging, err := newLoggingManager(config.LogConfig{Format: "text", Level: "debug"},
		singleLogDestinationFactory{destination: destination})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestLogging(t, logging) })
	return newProductionRuntimeReporter(logging, &productionState{member: "member-a"}, builderTime)
}

func assertLogFragments(t *testing.T, output string, fragments []string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(output, fragment) {
			t.Fatalf("log missing %q from %q", fragment, output)
		}
	}
}

func assertLogExcludes(t *testing.T, output string, values []string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(output, value) {
			t.Fatalf("log disclosed protected value %q in %q", value, output)
		}
	}
}

func TestGitHubManagerObserverPublishesProgressAndFailureDetail(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	observer := newGitHubManagerObserver(reporter.reportMaintenance, builderTime)

	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "source_reconciliation", Stage: "configure_repositories", Outcome: "progress",
		Current: 8, Total: 150})
	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "source_reconciliation", Stage: "reconciliation", Outcome: "failed",
		Repository: "https://github.com/acme/repository", Err: errors.New("GitHub returned 503")})

	output := destination.String()
	assertLogFragments(t, output, []string{
		"msg=\"RunnerFoundry progress event\" operation=source_reconciliation member=member-a stage=configure_repositories outcome=progress current=8 total=150",
		"level=ERROR msg=\"RunnerFoundry operational event\" operation=source_reconciliation member=member-a stage=reconciliation outcome=failed repository=https://github.com/acme/repository error_category=operation",
		"level=DEBUG msg=\"RunnerFoundry progress event\" operation=source_reconciliation member=member-a stage=reconciliation outcome=failed repository=https://github.com/acme/repository diagnostic=\"GitHub returned 503\"",
	})
}

func TestOperationalReporterPublishesProgressWithoutChangingHealth(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	notifier := &recordingStatusNotifier{}
	reporter.notifier = notifier

	reporter.ReportProgress(slog.LevelInfo, operationalEvent{
		operation: "source_reconciliation", stage: "configure_repositories", outcome: "progress",
		current: 2, total: 150, duration: 1250 * time.Millisecond,
	})
	reporter.ReportProgress(slog.LevelDebug, operationalEvent{
		operation: "source_reconciliation", stage: "configure_repository", outcome: "started",
		current: 2, total: 150, repository: "https://github.com/acme/private",
		endpoint: "docker:member-a", sourceRequestID: 42,
	})

	output := destination.String()
	assertLogFragments(t, output, []string{
		"msg=\"RunnerFoundry progress event\" operation=source_reconciliation member=member-a stage=configure_repositories outcome=progress current=2 total=150 duration_ms=1250",
		"level=DEBUG msg=\"RunnerFoundry progress event\" operation=source_reconciliation member=member-a stage=configure_repository outcome=started current=2 total=150 repository=https://github.com/acme/private endpoint=docker:member-a source_request_id=42",
	})
	if len(notifier.statuses) != 0 || len(reporter.failures) != 0 {
		t.Fatalf("progress changed health: statuses=%#v failures=%#v", notifier.statuses, reporter.failures)
	}
	if got := (progressEventEnvelope{event: operationalEvent{operation: "test"}}).Error(); got != "progress event: test" {
		t.Fatalf("progress envelope error = %q", got)
	}
}

func TestProviderObserverUsesDebugLoggingAfterProviderProtection(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	observer := newProviderObserver(reporter.reportMaintenance)
	observer(execution.ProviderActivity{Operation: "start", Outcome: "started", RunnerPool: "linux", EndpointID: "docker:member-a"})
	observer(execution.ProviderActivity{Operation: "start", Outcome: "failed", RunnerPool: "linux", EndpointID: "docker:member-a",
		Duration: time.Second, Err: errors.New("protected provider failure")})
	output := destination.String()
	assertLogFragments(t, output, []string{"operation=provider_start", "endpoint=docker:member-a", "diagnostic=\"protected provider failure\""})
}

func TestLoggingProtectsOnlyTheKnownCredentialBoundaries(t *testing.T) {
	privateKey := "github-private-key-material"
	postgresDSN := "postgres://operator:password@database.example/runnerfoundry"
	jit := "jit-configuration-material"
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	var captured transport.Credentials
	_, err := buildProductionGitHubManagerWithDependencies(config.Config{GitHub: config.GitHubConfig{App: config.GitHubAppConfig{
		AppID: 1, PrivateKeyFile: "in-memory-key",
	}}}, newBuilderStoreFake(), reporter.reportMaintenance, productionDependencies{
		readFile: func(string) ([]byte, error) { return []byte(privateKey), nil },
		newDiscoverer: func(credentials transport.Credentials, _ http.RoundTripper) (transport.Discoverer, error) {
			captured = credentials
			return transport.Discoverer{API: emptyDiscoveryAPI{}}, nil
		},
		newManager:  githubsource.NewManager,
		currentTime: builderTime,
	})
	if err != nil || captured.PrivateKey != privateKey {
		t.Fatalf("GitHub credential boundary = (%q, %v)", captured.PrivateKey, err)
	}
	reportOperational(reporter.reportMaintenance, operationalEvent{operation: "postgres", outcome: "failed", err: errors.New(postgresDSN)})
	provider := execution.WithOperationDeadlines(confidentialityProvider{}, newProviderObserver(reporter.reportMaintenance))
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "linux", EndpointID: "test:endpoint",
		Bootstrap: execution.NewBootstrap([]byte(jit))}); err != nil {
		t.Fatal(err)
	}
	output := destination.String()
	assertLogExcludes(t, output, []string{privateKey, postgresDSN, jit})
}

type emptyDiscoveryAPI struct{}

func (emptyDiscoveryAPI) ListInstallations(context.Context, int) ([]transport.InstallationID, int, error) {
	return nil, 0, nil
}
func (emptyDiscoveryAPI) ListRepositories(context.Context, transport.InstallationID, int) ([]string, int, error) {
	return nil, 0, nil
}

type confidentialityProvider struct{}

func (confidentialityProvider) EndpointID() execution.InventoryEndpointID           { return "test:endpoint" }
func (confidentialityProvider) Validate(context.Context) error                      { return nil }
func (confidentialityProvider) Start(context.Context, execution.StartRequest) error { return nil }
func (confidentialityProvider) Refresh(context.Context) error                       { return nil }
func (confidentialityProvider) Inspect(context.Context) ([]execution.RunnerInstance, error) {
	return nil, nil
}
func (confidentialityProvider) InspectEndpoint(context.Context, execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	return nil, nil
}
func (confidentialityProvider) KnowsEndpoint(execution.InventoryEndpointID) bool { return true }
func (confidentialityProvider) CanInspectEndpointForMember(execution.InventoryEndpointID, core.MemberID) bool {
	return true
}
func (confidentialityProvider) Destroy(context.Context, execution.RunnerInstance) error { return nil }

func TestOperationalReporterPublishesHealthAndRateLimitedTextEvents(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, now := newOperationalReporterFixture(t, "text", destination)
	notifier := &recordingStatusNotifier{}
	reporter.notifier = notifier
	exerciseOperationalRateLimiting(reporter, now)
	assertOperationalTextSnapshot(t, destination.String())
	assertOperationalStatusSnapshot(t, notifier.statuses)
}

func TestMaintenanceGenerationAndReporterOwnRecurrenceInOnePlace(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, now := newOperationalReporterFixture(t, "text", destination)
	generation := newMaintenanceGeneration(
		&lifecycleCycleEngineFake{},
		&manualCycleTicker{ticks: make(chan time.Time)},
		reporter.reportMaintenance,
		nil,
		nil,
	)
	wantErr := errors.New("cycle failed")
	generation.reportCycle(wantErr)
	*now = (*now).Add(time.Minute)
	generation.reportCycle(wantErr)
	*now = (*now).Add(4 * time.Minute)
	generation.reportCycle(wantErr)
	*now = (*now).Add(time.Minute)
	generation.reportCycle(nil)

	output := destination.String()
	for _, snapshot := range []string{
		"operation=maintenance member=member-a outcome=failed error_category=operation recurrence_count=1",
		"operation=maintenance member=member-a outcome=failed error_category=operation recurrence_count=3",
		"operation=maintenance member=member-a outcome=recovered recurrence_count=3 failure_duration=6m0s",
	} {
		if !strings.Contains(output, snapshot) {
			t.Fatalf("end-to-end log snapshot %q missing from %q", snapshot, output)
		}
	}
	if strings.Count(output, "outcome=failed") != 2 {
		t.Fatalf("failure log count = %d, want immediate and five-minute recurrence", strings.Count(output, "outcome=failed"))
	}
}

func exerciseOperationalRateLimiting(reporter *productionRuntimeReporter, now *time.Time) {
	reporter.Report(operationalEvent{operation: "leadership", outcome: "acquired", leadership: leadershipLeader})
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("secret detail")})
	*now = (*now).Add(time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("secret detail")})
	*now = (*now).Add(5 * time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("secret detail")})
	*now = (*now).Add(time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "failed", err: errors.New("changed detail")})
	*now = (*now).Add(time.Minute)
	reporter.Report(operationalEvent{operation: "maintenance", outcome: "recovered"})
}

func assertOperationalTextSnapshot(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "secret detail") || strings.Contains(output, "changed detail") {
		t.Fatalf("log exposed error detail: %q", output)
	}
	for _, snapshot := range []string{
		"operation=leadership member=member-a outcome=acquired leadership=leader",
		"operation=maintenance member=member-a outcome=failed error_category=operation recurrence_count=1",
		"operation=maintenance member=member-a outcome=failed error_category=operation recurrence_count=3",
		"operation=maintenance member=member-a outcome=recovered recurrence_count=1 failure_duration=1m0s",
	} {
		if !strings.Contains(output, snapshot) {
			t.Fatalf("text log snapshot %q missing from %q", snapshot, output)
		}
	}
}

func assertOperationalStatusSnapshot(t *testing.T, statuses []operationalStatus) {
	t.Helper()
	wantStatuses := []operationalStatus{
		{health: healthOperational, leadership: leadershipLeader},
		{health: healthDegraded, leadership: leadershipLeader},
		{health: healthOperational, leadership: leadershipLeader},
	}
	if len(statuses) != len(wantStatuses) {
		t.Fatalf("statuses = %#v, want %#v", statuses, wantStatuses)
	}
	for index := range wantStatuses {
		if statuses[index] != wantStatuses[index] {
			t.Fatalf("status %d = %#v, want %#v", index, statuses[index], wantStatuses[index])
		}
	}
}

func TestOperationalReporterPublishesStructuredJSONIdentifiers(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, _ := newOperationalReporterFixture(t, "json", destination)
	jobID := core.JobID{1}
	reporter.Report(operationalEvent{operation: "runner_retry", outcome: "source_terminal",
		runnerPool: "linux", jobID: jobID, count: 2, capacity: 3})

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(destination.String())), &record); err != nil {
		t.Fatalf("decode JSON log: %v", err)
	}
	want := map[string]any{"operation": "runner_retry", "member": "member-a", "outcome": "source_terminal",
		"runner_pool": "linux", "job_id": jobID.String(), "count": float64(2), "capacity": float64(3)}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("JSON field %q = %#v, want %#v in %#v", key, record[key], value, record)
		}
	}
}

func TestOperationalReporterHandlesEnvelopesRecoveryAndNotificationFailure(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter, _ := newOperationalReporterFixture(t, "text", destination)
	reporter.notifier = &failingStatusNotifier{}
	reporter.reportMaintenance(operationalEventEnvelope{event: operationalEvent{operation: "reload", outcome: "rejected"}})
	reporter.Report(operationalEvent{operation: "claim", outcome: "failed", err: errors.New("first")})
	reporter.Report(operationalEvent{operation: "claim", outcome: "claimed"})
	reporter.Report(operationalEvent{operation: "unknown", outcome: "recovered"})
	reporter.Report(operationalEvent{operation: "explicit", outcome: "failed", err: errors.New("hidden"), errorCategory: "protocol"})

	output := destination.String()
	for _, want := range []string{"operation=reload", "outcome=rejected", "operation=claim", "outcome=recovered",
		"error_category=protocol", "operation=status"} {
		if !strings.Contains(output, want) {
			t.Fatalf("log %q does not contain %q", output, want)
		}
	}
	if (operationalEventEnvelope{event: operationalEvent{operation: "reload"}}).Error() != "operational event: reload" {
		t.Fatal("unexpected envelope description")
	}
}

func TestPoolOperationalEventsDescribeOutcomesAndCleanup(t *testing.T) {
	events := make([]operationalEvent, 0)
	report := func(err error) {
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("report = %T, want operational envelope", err)
		}
		events = append(events, envelope.event)
	}
	jobID := core.JobID{1}
	reportProcessOutcome(report, "linux", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRunnerStarted, JobID: jobID}, nil)
	reportProcessOutcome(report, "linux", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry,
		RetryOutcome: core.RetrySourceTerminal, JobID: jobID}, errors.New("retry"))
	reportProcessOutcome(report, "linux", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil)
	reportCleanup(report, "linux", runtimeengine.PoolReconciliation{})
	reportCleanup(report, "linux", runtimeengine.PoolReconciliation{RemovedRunners: 1, RemovedProcessing: 1})
	if len(events) != 5 || events[0].operation != "claim" || events[1].operation != "runner_start" ||
		events[2].operation != "claim" || events[3].operation != "runner_retry" || events[4].operation != "cleanup" {
		t.Fatalf("events = %#v", events)
	}
}

func newOperationalReporterFixture(
	t *testing.T,
	format string,
	destination *memoryLogDestination,
) (*productionRuntimeReporter, *time.Time) {
	t.Helper()
	logging, err := newLoggingManager(config.LogConfig{Format: format}, singleLogDestinationFactory{destination: destination})
	if err != nil {
		t.Fatalf("configure logging: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := logging.Close(); closeErr != nil {
			t.Errorf("close logging: %v", closeErr)
		}
	})
	now := time.Unix(1_700_000_000, 0).UTC()
	reporter := newProductionRuntimeReporter(logging, &productionState{member: "member-a"},
		func() time.Time { return now })
	return reporter, &now
}

type recordingStatusNotifier struct{ statuses []operationalStatus }

func (*recordingStatusNotifier) Ready(context.Context) error     { return nil }
func (*recordingStatusNotifier) Reloading(context.Context) error { return nil }
func (notifier *recordingStatusNotifier) Status(_ context.Context, status operationalStatus) error {
	notifier.statuses = append(notifier.statuses, status)
	return nil
}

type failingStatusNotifier struct{}

func (*failingStatusNotifier) Ready(context.Context) error     { return nil }
func (*failingStatusNotifier) Reloading(context.Context) error { return nil }
func (*failingStatusNotifier) Status(context.Context, operationalStatus) error {
	return errors.New("notify failed")
}
