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

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
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
		"msg=\"Reconciling GitHub repositories: 8/150\" type=progress_event component=github_registration operation=source_reconciliation member=member-a stage=configure_repositories outcome=progress current=8 total=150",
		"level=ERROR msg=\"GitHub repository reconciliation failed for https://github.com/acme/repository\" type=operational_event component=github_registration operation=source_reconciliation member=member-a stage=reconciliation outcome=failed repository=https://github.com/acme/repository error=\"GitHub returned 503\" error_category=operation",
	})
}

func TestGitHubAppRecoveryDoesNotClearAnotherAppsFailure(t *testing.T) {
	reporter := newDebugRuntimeReporter(t, &memoryLogDestination{})
	first := newGitHubManagerObserverForApp(reporter.reportMaintenance, builderTime, 11)
	second := newGitHubManagerObserverForApp(reporter.reportMaintenance, builderTime, 22)
	failure := githubsource.ManagerActivity{Operation: "source_reconciliation", Stage: "reconciliation",
		Outcome: "failed", Err: errors.New("unavailable")}
	first.Report(failure)
	second.Report(failure)
	first.Report(githubsource.ManagerActivity{Operation: "source_reconciliation", Stage: "reconciliation", Outcome: "completed"})
	if len(reporter.failures) != 1 {
		t.Fatalf("remaining App failures = %#v", reporter.failures)
	}
	for _, remaining := range reporter.failures {
		if remaining.event.githubAppID != 22 {
			t.Fatalf("remaining failure belongs to App %d", remaining.event.githubAppID)
		}
	}
}

func TestGitHubAssignedDemandLoggingNamesChangeAndCarriesCounts(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	observer := newGitHubManagerObserver(reporter.reportMaintenance, builderTime)

	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "assigned_demand", Stage: "reconcile", Outcome: "completed",
		Repository: "https://github.com/example/project", RunnerPool: "linux",
		InstallationID: 17, ScaleSetID: 23, DesiredJobs: 12, ActiveJobs: 7,
		AvailableJobs: 2, AcquiredJobs: 3, RunningJobs: 4, RegisteredRunners: 6,
		BusyRunners: 4, IdleRunners: 2, QueuedJobs: 4, ProcessingJobs: 3, JobsAdded: 5})
	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityDebug,
		Operation: "assigned_demand", Stage: "reconcile", Outcome: "failed",
		Repository: "https://github.com/example/project", RunnerPool: "linux",
		InstallationID: 17, ScaleSetID: 23, DesiredJobs: 12,
		Err: errors.New("state operation failed")})

	assertLogFragments(t, destination.String(), []string{
		"Recovered 5 assigned GitHub jobs for https://github.com/example/project in Runner Pool linux",
		"component=github_messages operation=assigned_demand", "installation_id=17", "scale_set_id=23",
		"desired_jobs=12", "active_jobs=7", "queued_jobs=4", "processing_jobs=3", "jobs_added=5",
		"available_jobs=2", "acquired_jobs=3", "running_jobs=4", "registered_runners=6",
		"busy_runners=4", "idle_runners=2",
		"Failed to reconcile assigned GitHub demand for https://github.com/example/project in Runner Pool linux",
	})
}

func TestGitHubLifecycleFailureLoggingText(t *testing.T) {
	output := githubLifecycleFailureLog(t, "text")
	assertLogFragments(t, output, []string{
		"SQLite transaction failed while attempting to record terminal state for GitHub runner runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w",
		"repository=https://github.com/example/project", "runner_pool=linux", "message_id=91",
		"runner_name=runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w",
		"github_event_type=JobCompleted", "state_operation=terminalize_job",
	})
}

func TestGitHubLifecycleFailureLoggingJSON(t *testing.T) {
	var record map[string]any
	if err := json.Unmarshal([]byte(githubLifecycleFailureLog(t, "json")), &record); err != nil {
		t.Fatalf("decode lifecycle JSON log: %v", err)
	}
	assertJSONFields(t, record, map[string]any{
		"msg": "SQLite transaction failed while attempting to record terminal state for GitHub runner " +
			"runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w for https://github.com/example/project in Runner Pool linux",
		"repository": "https://github.com/example/project", "runner_pool": "linux", "message_id": float64(91),
		"runner_name":       "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w",
		"github_event_type": "JobCompleted", "state_operation": "terminalize_job",
	})
}

func githubLifecycleFailureLog(t *testing.T, format string) string {
	t.Helper()
	destination := &memoryLogDestination{}
	reporter := newOperationalReporterFixture(t, format, destination)
	observer := newGitHubManagerObserver(reporter.reportMaintenance, builderTime)
	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "job_observation", Stage: "persist_terminal", Outcome: "failed",
		Repository: "https://github.com/example/project", RunnerPool: "linux", MessageID: 91,
		RunnerName: "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w", EventType: "JobCompleted",
		StateBackend: "sqlite", StateOperation: "terminalize_job", Err: errors.New("state operation failed"),
	})
	return strings.TrimSpace(destination.String())
}

func assertJSONFields(t *testing.T, record map[string]any, want map[string]any) {
	t.Helper()
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("JSON field %q = %#v, want %#v in %#v", key, record[key], value, record)
		}
	}
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
		"msg=\"Reconciling GitHub repositories: 2/150\" type=progress_event component=github_registration operation=source_reconciliation member=member-a stage=configure_repositories outcome=progress current=2 total=150 duration_ms=1250",
		"level=DEBUG msg=\"GitHub repository reconciliation started: configure repository\" type=progress_event component=github_registration operation=source_reconciliation member=member-a stage=configure_repository outcome=started current=2 total=150 repository=https://github.com/acme/private endpoint=docker:member-a source_request_id=42",
	})
	if len(notifier.statuses) != 0 || len(reporter.failures) != 0 {
		t.Fatalf("progress changed health: statuses=%#v failures=%#v", notifier.statuses, reporter.failures)
	}
	if got := (progressEventEnvelope{event: operationalEvent{operation: "test"}}).Error(); got != "progress event: test" {
		t.Fatalf("progress envelope error = %q", got)
	}
}

func TestGitHubIgnoredLifecycleIsWarningEvidenceWithoutHealthFailure(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	notifier := &recordingStatusNotifier{}
	reporter.notifier = notifier
	observer := newGitHubManagerObserver(reporter.reportMaintenance, builderTime)
	reporter.Report(operationalEvent{operation: "job_observation", stage: "validate_terminal", outcome: "failed",
		repository: "https://github.com/example/project", runnerPool: "linux", err: errors.New("prior failure")})

	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "job_observation", Stage: "validate_terminal", Outcome: "ignored",
		Repository: "https://github.com/example/project", RunnerPool: "linux", MessageID: 91,
		EventType: "JobCompleted", GitHubJobID: "github-job-17", WorkflowRunID: 81,
		GitHubRunnerID: 72, IdentityReason: "missing_identity",
	})

	assertLogFragments(t, destination.String(), []string{
		"level=WARN msg=\"Cannot correlate GitHub JobCompleted event from message 91 for https://github.com/example/project in Runner Pool linux because it has neither a positive runner request ID nor a runner name\"",
		"type=operational_event component=github_messages operation=job_observation member=member-a stage=validate_terminal outcome=ignored",
		"runner_pool=linux message_id=91 github_event_type=JobCompleted github_job_id=github-job-17 workflow_run_id=81 github_runner_id=72 repository=https://github.com/example/project identity_reason=missing_identity",
	})
	if len(reporter.failures) != 1 {
		t.Fatalf("ignored lifecycle observation changed the prior active failure: %#v", reporter.failures)
	}
	if len(notifier.statuses) != 1 || notifier.statuses[0].health != healthDegraded {
		t.Fatalf("ignored lifecycle observation changed health: %#v", notifier.statuses)
	}
}

func TestGitHubIgnoredLifecycleJSONCarriesSourceMetadata(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newOperationalReporterFixture(t, "json", destination)
	observer := newGitHubManagerObserver(reporter.reportMaintenance, builderTime)
	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "job_ingestion", Stage: "validate_available", Outcome: "ignored",
		Repository: "https://github.com/example/project", RunnerPool: "linux", MessageID: 92,
		EventType: "JobAvailable", SourceRequestIDSet: true, GitHubJobID: "github-job-18", WorkflowRunID: 82,
		IdentityReason: "non_positive_request_id",
	})

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(destination.String())), &record); err != nil {
		t.Fatalf("decode JSON log: %v", err)
	}
	assertJSONFields(t, record, map[string]any{
		"level": "WARN", "msg": "Cannot acquire GitHub JobAvailable event from message 92 for https://github.com/example/project " +
			"in Runner Pool linux because it has no runner request ID",
		"type": "operational_event", "component": "github_messages", "operation": "job_ingestion",
		"stage": "validate_available", "outcome": "ignored", "runner_pool": "linux",
		"message_id": float64(92), "github_event_type": "JobAvailable", "github_job_id": "github-job-18",
		"workflow_run_id": float64(82), "repository": "https://github.com/example/project",
		"source_request_id": float64(0), "identity_reason": "non_positive_request_id",
	})
}

func TestProviderObserverUsesDebugLoggingAfterProviderProtection(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	observer := newProviderObserver(reporter.reportMaintenance)
	observer(runtimeengine.ProviderActivity{Operation: "start", Outcome: "started", RunnerPool: "linux", EndpointID: "docker:member-a"}, "runner")
	observer(runtimeengine.ProviderActivity{Operation: "start", Outcome: "failed", RunnerPool: "linux", EndpointID: "docker:member-a",
		Provider: "docker", JobID: core.JobID{1}, Duration: time.Second, Err: errors.New("protected provider failure")}, "runner")
	output := destination.String()
	assertLogFragments(t, output, []string{
		"level=ERROR msg=\"Docker Compose runner start failed for job 01000000-0000-0000-0000-000000000000 in Runner Pool linux\"",
		"component=execution_provider operation=provider_start", "runner_pool=linux",
		"endpoint=docker:member-a", "error=\"protected provider failure\"", "provider=docker",
	})
}

func TestDurableStateFailureNamesBackendOperationAndSubjectWithoutDriverDetail(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	observer := newGitHubManagerObserver(reporter.reportMaintenance, builderTime)
	driverDetail := "postgres://operator:password@database.example/runnerfoundry"

	observer.Report(githubsource.ManagerActivity{Visibility: githubsource.ActivityInfo,
		Operation: "job_ingestion", Stage: "persist_job", Outcome: "failed",
		Repository: "https://github.com/example/project", RunnerPool: "linux", SourceRequestID: 42,
		StateBackend: "postgresql", StateOperation: "enqueue_job", Err: errors.New(driverDetail),
	})

	output := destination.String()
	assertLogFragments(t, output, []string{
		"level=ERROR msg=\"PostgreSQL transaction failed while enqueuing GitHub runner request 42 for https://github.com/example/project in Runner Pool linux\"",
		"component=github_messages operation=job_ingestion", "state_backend=postgresql", "state_operation=enqueue_job",
	})
	assertLogExcludes(t, output, []string{driverDetail})
}

func TestLoggingProtectsOnlyTheKnownCredentialBoundaries(t *testing.T) {
	privateKey := "github-private-key-material"
	postgresDSN := "postgres://operator:password@database.example/runnerfoundry"
	jit := "jit-configuration-material"
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	assertGitHubPrivateKeyRemainsAtBoundary(t, reporter, privateKey)
	reportOperational(reporter.reportMaintenance, operationalEvent{operation: "postgres", outcome: "failed", err: errors.New(postgresDSN)})
	assertJITRemainsAtProviderBoundary(t, reporter, jit)
	assertLogExcludes(t, destination.String(), []string{privateKey, postgresDSN, jit})
}

func assertGitHubPrivateKeyRemainsAtBoundary(t *testing.T, reporter *productionRuntimeReporter, privateKey string) {
	t.Helper()
	var captured transport.Credentials
	_, err := buildProductionGitHubManagerWithDependencies(config.Config{GitHub: config.GitHubConfig{Apps: []config.GitHubAppConfig{{
		AppID: 1, PrivateKeyFile: "in-memory-key",
	}}}}, newBuilderStoreFake(), reporter.reportMaintenance, productionDependencies{
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
}

func TestGitHubAppObserverAttachesCredentialIdentityWithoutSecrets(t *testing.T) {
	var reported error
	observer := newGitHubManagerObserverForApp(func(err error) { reported = err }, builderTime, 4273059)
	observer.Report(githubsource.ManagerActivity{Operation: "source_reconciliation", Stage: "repository_overlap",
		Outcome: "ignored", Repository: "https://github.com/stateshifters/runnerfoundry"})
	var envelope operationalEventEnvelope
	if !errors.As(reported, &envelope) || envelope.event.githubAppID != 4273059 || envelope.event.repository == "" {
		t.Fatalf("App-scoped operational event = %#v", reported)
	}
}

func assertJITRemainsAtProviderBoundary(t *testing.T, reporter *productionRuntimeReporter, jit string) {
	t.Helper()
	provider := runtimeengine.WithOperationDeadlines(
		confidentialityProvider{}, "test",
		observeProviderActivity(newProviderObserver(reporter.reportMaintenance), ""),
	)
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "linux", EndpointID: "test:endpoint",
		Bootstrap: core.NewBootstrap([]byte(jit))}); err != nil {
		t.Fatal(err)
	}
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

func TestOperationalReporterPublishesStructuredJSONIdentifiers(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newOperationalReporterFixture(t, "json", destination)
	jobID := core.JobID{1}
	reporter.Report(operationalEvent{operation: "runner_retry", outcome: "source_terminal",
		runnerPool: "linux", jobID: jobID, runnerID: "runner-1", runnerName: "external-runner",
		githubEventType: "JobCompleted", installationID: 11, runnerGroupID: 12,
		scaleSetID: 13, messageID: 14, count: 2, capacity: 3, provider: "docker", composeService: "runner",
		stateBackend: "postgresql", stateOperation: "retry_unstarted"})

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(destination.String())), &record); err != nil {
		t.Fatalf("decode JSON log: %v", err)
	}
	want := map[string]any{"operation": "runner_retry", "member": "member-a", "outcome": "source_terminal",
		"runner_pool": "linux", "job_id": jobID.String(), "runner_id": "runner-1",
		"runner_name": "external-runner", "github_event_type": "JobCompleted", "installation_id": float64(11),
		"runner_group_id": float64(12), "scale_set_id": float64(13), "message_id": float64(14),
		"count": float64(2), "capacity": float64(3), "provider": "docker", "compose_service": "runner",
		"state_backend": "postgresql", "state_operation": "retry_unstarted"}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("JSON field %q = %#v, want %#v in %#v", key, record[key], value, record)
		}
	}
}

func TestOperationalReporterHandlesEnvelopesRecoveryAndNotificationFailure(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newOperationalReporterFixture(t, "text", destination)
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

func TestOperationalReporterRecoversEverySubjectForAnOperation(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newOperationalReporterFixture(t, "text", destination)
	reporter.Report(operationalEvent{operation: "other", outcome: "failed", err: errors.New("other")})
	reporter.Report(operationalEvent{operation: "source_poll", outcome: "failed", repository: "first", err: errors.New("first")})
	reporter.Report(operationalEvent{operation: "source_poll", outcome: "failed", repository: "second", err: errors.New("second")})
	reporter.Report(operationalEvent{operation: "source_poll", outcome: "recovered"})
	if len(reporter.failures) != 1 {
		t.Fatalf("active failures after scoped operation recovery = %#v", reporter.failures)
	}
}

func TestOperationalErrorWrappersDescribeAndPreserveTheirCause(t *testing.T) {
	cause := errors.New("reported")
	if (reportedOperationalError{cause: cause}).Error() == "" {
		t.Fatal("error wrappers did not describe their purpose")
	}
	if !errors.Is(reportedOperationalError{cause: cause}, cause) {
		t.Fatal("reported operational error did not preserve its cause")
	}
	if markOperationallyReported(nil) != nil {
		t.Fatal("nil operational result was wrapped")
	}
	if operationalErrorWasReported(errors.Join(reportedOperationalError{cause: cause}, errors.New("unreported"))) {
		t.Fatal("mixed reported and unreported causes were classified as wholly reported")
	}
	if !operationalErrorWasReported(errors.Join(
		reportedOperationalError{cause: cause}, reportedOperationalError{cause: errors.New("also reported")},
	)) {
		t.Fatal("fully reported joined causes were not recognized")
	}
}

func TestPoolOperationalEventsDescribeProcessingOutcomes(t *testing.T) {
	events := make([]operationalEvent, 0)
	report := func(err error) {
		var envelope operationalEventEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("report = %T, want operational envelope", err)
		}
		events = append(events, envelope.event)
	}
	jobID := core.JobID{1}
	reportProcessOutcome(report, "linux", "docker", "docker:test", "sqlite", runtimeengine.ProcessResult{
		Outcome: runtimeengine.ProcessRunnerStarted, JobID: jobID}, nil)
	reportProcessOutcome(report, "linux", "docker", "docker:test", "sqlite", runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry,
		RetryOutcome: core.RetrySourceTerminal, JobID: jobID}, errors.New("retry"))
	reportProcessOutcome(report, "linux", "docker", "docker:test", "sqlite",
		runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessNoWork}, nil)
	if len(events) != 4 || events[0].operation != "claim" || events[1].operation != "runner_start" ||
		events[2].operation != "claim" || events[3].operation != "runner_retry" {
		t.Fatalf("events = %#v", events)
	}
}

func TestProvisioningFailureIdentifiesGitHubJobPoolProviderAndEndpoint(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	sourceID, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: "https://github.com/example/project", RunnerPool: "linux", RequestID: 42,
	})
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	jobID := core.JobID{1}
	job := core.Job{ID: jobID, Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}, RunnerPool: "linux"}

	reportProcessOutcome(reporter.reportMaintenance, "linux", "docker", "docker-compose:member-a", "postgresql",
		runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry, RetryOutcome: core.RetryRequeued,
			JobID: jobID, Job: job, FailedStage: "start_runner"},
		errors.New("protected Docker failure"),
	)

	assertLogFragments(t, destination.String(), []string{
		"level=ERROR msg=\"Failed to start Docker Compose runner for GitHub request 42 from https://github.com/example/project in Runner Pool linux\"",
		"component=job_queue operation=runner_retry",
		"runner_pool=linux job_id=01000000-0000-0000-0000-000000000000",
		"repository=https://github.com/example/project endpoint=docker-compose:member-a",
		"source_request_id=42",
		"provider=docker",
	})
}

type safeDiagnosticTestError struct{ diagnostic string }

func (failure safeDiagnosticTestError) Error() string                 { return "bootstrap failed" }
func (failure safeDiagnosticTestError) OperationalDiagnostic() string { return failure.diagnostic }

func TestProvisioningFailureIncludesOnlyOptInSafeDiagnostic(t *testing.T) {
	events := make([]operationalEvent, 0, 2)
	report := func(err error) {
		var envelope operationalEventEnvelope
		if errors.As(err, &envelope) {
			events = append(events, envelope.event)
		}
	}
	result := runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry, RetryOutcome: core.RetryRequeued,
		JobID: core.JobID{1}, FailedStage: "generate_bootstrap"}
	reportProcessOutcome(report, "linux", "docker", "docker:test", "sqlite", result,
		safeDiagnosticTestError{diagnostic: "GitHub conflict: runner exists"})
	if len(events) != 2 || events[1].diagnostic != "GitHub conflict: runner exists" {
		t.Fatalf("events = %#v", events)
	}
}

func TestGitHubJobContextRejectsInvalidOrMismatchedDurableIdentity(t *testing.T) {
	reported := make([]error, 0)
	report := func(err error) { reported = append(reported, err) }
	base := operationalEvent{runnerPool: "linux"}

	if got := addGitHubJobContext(report, base, core.Job{}); got != base {
		t.Fatalf("empty source changed context: %#v", got)
	}
	if got := addGitHubJobContext(report, base, core.Job{Source: core.SourceReference{Adapter: "other"}}); got != base {
		t.Fatalf("foreign source changed context: %#v", got)
	}
	_ = addGitHubJobContext(report, base, core.Job{
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: "invalid"}, RunnerPool: "linux",
	})
	sourceID, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: "https://github.com/example/project", RunnerPool: "windows", RequestID: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = addGitHubJobContext(report, base, core.Job{
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}, RunnerPool: "linux",
	})
	if len(reported) != 2 {
		t.Fatalf("identity failures = %d, want invalid and mismatched identities", len(reported))
	}
}

func TestDebugLoggingIdentifiesTheEventProducer(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)

	emitDebugProvenance(reporter.reportMaintenance)

	assertLogFragments(t, destination.String(), []string{
		"source.file=", "source.line=", "source.function=github.com/stateshifters/runnerfoundry/internal/daemon.emitDebugProvenance",
	})
}

func emitDebugProvenance(report func(error)) {
	reportProgress(report, slog.LevelDebug, operationalEvent{
		operation: "maintenance_cycle", stage: "scheduled", outcome: "started",
	})
}

func TestMaintenancePhaseReportsItsOwningFailure(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	engine := &UnifiedEngine{report: reporter.reportMaintenance, currentTime: builderTime}
	wantErr := errors.New("inventory endpoint unavailable")

	engine.phaseCompleted("refresh_inventory", "inventory", "linux", builderTime(), wantErr)

	output := destination.String()
	assertLogFragments(t, output, []string{
		"level=ERROR msg=\"Runner inventory refresh failed for Runner Pool linux\"",
		"component=runner_inventory operation=refresh_inventory", "stage=inventory outcome=failed",
		"runner_pool=linux", "error_category=operation",
	})
}

func TestMaintenancePhaseDoesNotRepeatAnOwningBoundaryFailure(t *testing.T) {
	destination := &memoryLogDestination{}
	reporter := newDebugRuntimeReporter(t, destination)
	engine := &UnifiedEngine{report: reporter.reportMaintenance, currentTime: builderTime}

	engine.phaseCompleted("leader_work", "source", "", builderTime(),
		markOperationallyReported(errors.New("GitHub boundary already reported this failure")))

	if strings.Contains(destination.String(), "level=ERROR") {
		t.Fatalf("reported boundary failure was repeated: %q", destination.String())
	}
}

func newOperationalReporterFixture(
	t *testing.T,
	format string,
	destination *memoryLogDestination,
) *productionRuntimeReporter {
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
	return reporter
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
