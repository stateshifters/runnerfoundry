//go:build e2e

package daemon

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubtransport "github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestProductContractGitHubPruningDeletesOnlyExactSafeStaleOwnership(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	exerciseGitHubPruningSafetyMatrix(t, githubPruningSQLiteState)
}

func TestProductContractGitHubPruningDeletesOnlyExactSafeStaleOwnershipPostgres(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	exerciseGitHubPruningSafetyMatrix(t, githubPruningPostgresState)
}

func exerciseGitHubPruningSafetyMatrix(t *testing.T, stateFactory githubPruningStateFactory) {
	t.Helper()
	exact := upstream.RunnerScaleSet{ID: 29, Name: "linux", RunnerGroupID: 11}
	scenarios := []githubPruningSafetyScenario{
		{name: "exact stale fingerprint", listed: []upstream.RunnerScaleSet{exact}, deleteStatus: http.StatusNoContent,
			wantDeleteAttempts: 1, wantOwnership: false},
		{name: "different ID is already absent", listed: []upstream.RunnerScaleSet{{ID: 30, Name: "linux", RunnerGroupID: 11}},
			wantOwnership: false},
		{name: "name mismatch is retained", listed: []upstream.RunnerScaleSet{{ID: 29, Name: "other", RunnerGroupID: 11}},
			wantOwnership: true},
		{name: "group mismatch is retained", listed: []upstream.RunnerScaleSet{{ID: 29, Name: "linux", RunnerGroupID: 12}},
			wantOwnership: true},
		{name: "delete reports already absent", listed: []upstream.RunnerScaleSet{exact}, deleteStatus: http.StatusNotFound,
			wantDeleteAttempts: 1, wantOwnership: false},
		{name: "pruning disabled retains active ownership", pruningDisabled: true, seedActive: true, wantOwnership: true,
			wantActive: true},
		{name: "live declaration retains ownership", liveDeclaration: true, listed: []upstream.RunnerScaleSet{exact},
			wantOwnership: true, wantActive: true},
		{name: "inconclusive discovery retains ownership", discoveryInconclusive: true, wantError: true,
			wantOwnership: true},
		{name: "delete failure retains ownership", listed: []upstream.RunnerScaleSet{exact}, deleteStatus: http.StatusBadRequest,
			wantDeleteAttempts: 1, wantError: true, wantPruneFailure: true, wantOwnership: true},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			exerciseGitHubPruningSafety(t, stateFactory(t), scenario)
		})
	}
}

type githubPruningStateFactory func(*testing.T) config.StateConfig

func githubPruningSQLiteState(t *testing.T) config.StateConfig {
	t.Helper()
	return config.StateConfig{Path: ":memory:"}
}

func githubPruningPostgresState(t *testing.T) config.StateConfig {
	t.Helper()
	fixture := e2ePostgresStart(t)
	settings := fixture.Settings()
	return config.StateConfig{Postgres: &settings}
}

type githubPruningSafetyScenario struct {
	name                  string
	listed                []upstream.RunnerScaleSet
	deleteStatus          int
	discoveryInconclusive bool
	pruningDisabled       bool
	liveDeclaration       bool
	seedActive            bool
	wantDeleteAttempts    int
	wantError             bool
	wantPruneFailure      bool
	wantOwnership         bool
	wantActive            bool
}

func exerciseGitHubPruningSafety(
	t *testing.T,
	stateConfig config.StateConfig,
	scenario githubPruningSafetyScenario,
) {
	t.Helper()
	ctx := context.Background()
	state := openGitHubPruningSafetyStore(ctx, t, stateConfig)
	host := newGitHubPruningHTTPBoundary(scenario)
	manager := newProductionTransportPruningManager(t, state.registrations, host)
	registration := githubPruningRegistration()
	seedGitHubPruningOwnership(ctx, t, state.registrations, registration)
	if !scenario.seedActive {
		if err := state.registrations.DeleteActiveRegistration(ctx, registration); err != nil {
			t.Fatalf("make GitHub ownership stale: %v", err)
		}
	}
	configured := githubPruningManagerConfig(ctx, t, state, manager, scenario)

	_, reconcileErr := manager.Reconcile(ctx, configured)
	assertGitHubPruningError(t, reconcileErr, scenario)
	assertGitHubPruningState(ctx, t, manager, host, scenario)
}

type githubPruningSafetyState struct {
	registrations      githubsource.RegistrationRepository
	publishMemberState func(context.Context, core.MemberID, time.Time, []core.RunnerPoolCapacity) error
}

func openGitHubPruningSafetyStore(
	ctx context.Context,
	t *testing.T,
	stateConfig config.StateConfig,
) *githubPruningSafetyState {
	t.Helper()
	dependencies := newProductionDependencies()
	dependencies.readHostname = func() (string, error) { return "github-pruning-member", nil }
	state, err := openProductionStateWithDependencies(ctx, config.Config{State: stateConfig}, dependencies)
	if err != nil {
		t.Fatalf("open production GitHub pruning state: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := closeRuntimeResources(ctx, state.closers); closeErr != nil {
			t.Errorf("close production GitHub pruning state: %v", closeErr)
		}
	})
	return &githubPruningSafetyState{
		registrations: state.registrationStore, publishMemberState: state.store.PublishMemberState,
	}
}

func newProductionTransportPruningManager(
	t *testing.T,
	store githubsource.RegistrationRepository,
	host *githubPruningHTTPBoundary,
) *githubsource.Manager {
	t.Helper()
	httpTransport := githubPruningHTTPTransport(t, host)
	credentials := githubtransport.Credentials{AppID: 1, PrivateKey: string(productionCompositionPrivateKey(t))}
	discoverer, err := githubtransport.NewDiscoverer(credentials, httpTransport)
	if err != nil {
		t.Fatalf("build production GitHub discoverer: %v", err)
	}
	factory := func(
		_ context.Context,
		repository githubtransport.Repository,
	) (githubsource.ManagerClient, error) {
		client, clientErr := githubtransport.NewClient(repository, credentials, func() *http.Transport {
			return githubPruningHTTPTransport(t, host)
		})
		if clientErr != nil {
			return githubsource.ManagerClient{}, fmt.Errorf("build production GitHub client: %w", clientErr)
		}
		return githubsource.NewTransportManagerClient(client), nil
	}
	manager, err := githubsource.NewManager(discoverer, factory, e2eAppRegistrations(t, store), githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("build production transport manager: %v", err)
	}
	return manager
}

func githubPruningHTTPTransport(t *testing.T, host *githubPruningHTTPBoundary) *http.Transport {
	t.Helper()
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("default HTTP transport = %T, want *http.Transport", http.DefaultTransport)
	}
	httpTransport := defaultTransport.Clone()
	httpTransport.Proxy = nil
	httpTransport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
		return nil, fmt.Errorf("GitHub pruning transport forbids %s dial to %s", network, address)
	}
	httpTransport.DialTLSContext = func(_ context.Context, network, address string) (net.Conn, error) {
		return nil, fmt.Errorf("GitHub pruning transport forbids TLS %s dial to %s", network, address)
	}
	httpTransport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	httpTransport.Protocols = new(http.Protocols)
	httpTransport.ForceAttemptHTTP2 = false
	httpTransport.RegisterProtocol("https", host)
	t.Cleanup(httpTransport.CloseIdleConnections)
	return httpTransport
}

func TestGitHubPruningHTTPTransportFailsClosed(t *testing.T) {
	transport := githubPruningHTTPTransport(t, newGitHubPruningHTTPBoundary(githubPruningSafetyScenario{}))
	assertProductionCompositionTransportIsClosed(t, transport)
	for _, address := range []string{"http://unregistered.invalid/", "https://unregistered.invalid/"} {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, address, http.NoBody)
		if err != nil {
			t.Fatalf("build pruning fail-closed request for %s: %v", address, err)
		}
		response, requestErr := transport.RoundTrip(request)
		if response != nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Fatalf("close unexpected pruning response for %s: %v", address, closeErr)
			}
		}
		if requestErr == nil {
			t.Fatalf("unregistered pruning route %s unexpectedly succeeded", address)
		}
	}
}

func githubPruningRegistration() githubsource.Registration {
	return githubsource.Registration{
		GitHubAppID: 1, RepositoryURL: "https://github.com/acme/widgets", RunnerPool: "linux",
		InstallationID: 71, RunnerGroupID: 11, ScaleSetID: 29,
	}
}

func seedGitHubPruningOwnership(
	ctx context.Context,
	t *testing.T,
	store githubsource.RegistrationRepository,
	registration githubsource.Registration,
) {
	t.Helper()
	if err := store.UpsertRegistration(ctx, registration); err != nil {
		t.Fatalf("seed GitHub ownership: %v", err)
	}
}

func githubPruningManagerConfig(
	ctx context.Context,
	t *testing.T,
	state *githubPruningSafetyState,
	manager *githubsource.Manager,
	scenario githubPruningSafetyScenario,
) githubsource.ManagerConfig {
	t.Helper()
	configured := githubsource.ManagerConfig{Prune: !scenario.pruningDisabled}
	if !scenario.liveDeclaration {
		return configured
	}
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	memberID := core.MemberID("pruning-member")
	if err := state.publishMemberState(ctx, memberID, now, nil); err != nil {
		t.Fatalf("publish pruning member: %v", err)
	}
	declarations := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: "linux", RunnerGroup: "default",
	}}}
	if err := manager.PublishRunnerPoolDeclarations(ctx, memberID, declarations); err != nil {
		t.Fatalf("publish live pruning declaration: %v", err)
	}
	snapshot, err := manager.ClusterRunnerPools(ctx, now.Add(-runtimeMemberTTL))
	if err != nil || len(snapshot.RunnerPools) != 1 {
		t.Fatalf("live pruning declarations = %+v, error = %v", snapshot, err)
	}
	configured.RunnerPools = snapshot.RunnerPools
	return configured
}

func assertGitHubPruningError(t *testing.T, actual error, scenario githubPruningSafetyScenario) {
	t.Helper()
	if (actual != nil) != scenario.wantError {
		t.Fatalf("pruning error = %v, want error %t", actual, scenario.wantError)
	}
	if githubsource.IsPruneFailure(actual) != scenario.wantPruneFailure {
		t.Fatalf("pruning error classification = %v, want prune failure %t", actual, scenario.wantPruneFailure)
	}
}

func assertGitHubPruningState(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	host *githubPruningHTTPBoundary,
	scenario githubPruningSafetyScenario,
) {
	t.Helper()
	snapshot, err := manager.RegistrationSnapshot(ctx)
	if err != nil {
		t.Fatalf("read pruning registration state: %v", err)
	}
	if (len(snapshot.Owned) == 1) != scenario.wantOwnership ||
		(len(snapshot.Active) == 1) != scenario.wantActive {
		t.Fatalf("pruning registration state = %+v, want ownership=%t active=%t",
			snapshot, scenario.wantOwnership, scenario.wantActive)
	}
	deleteAttempts, unexpected := host.observations()
	if deleteAttempts != scenario.wantDeleteAttempts || len(unexpected) != 0 {
		t.Fatalf("pruning HTTP observations = deletes %d unexpected %v, want deletes %d",
			deleteAttempts, unexpected, scenario.wantDeleteAttempts)
	}
}

type githubPruningHTTPBoundary struct {
	mutex             sync.Mutex
	scenario          githubPruningSafetyScenario
	deleteBarrier     *githubPruningDeleteBarrier
	deleteAttempts    int
	unexpectedRequest []string
}

func newGitHubPruningHTTPBoundary(scenario githubPruningSafetyScenario) *githubPruningHTTPBoundary {
	return &githubPruningHTTPBoundary{scenario: scenario}
}

type githubPruningDeleteBarrier struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func (host *githubPruningHTTPBoundary) blockNextDelete(t *testing.T) *githubPruningDeleteBarrier {
	t.Helper()
	barrier := &githubPruningDeleteBarrier{started: make(chan struct{}), release: make(chan struct{})}
	host.mutex.Lock()
	host.deleteBarrier = barrier
	host.mutex.Unlock()
	t.Cleanup(barrier.Release)
	return barrier
}

func (barrier *githubPruningDeleteBarrier) wait(ctx context.Context) error {
	barrier.startedOnce.Do(func() { close(barrier.started) })
	select {
	case <-barrier.release:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("GitHub pruning delete barrier canceled: %w", ctx.Err())
	}
}

func (barrier *githubPruningDeleteBarrier) Release() {
	barrier.releaseOnce.Do(func() { close(barrier.release) })
}

func (host *githubPruningHTTPBoundary) RoundTrip(request *http.Request) (*http.Response, error) {
	status, body, err := host.response(request)
	if err != nil {
		host.mutex.Lock()
		host.unexpectedRequest = append(host.unexpectedRequest, err.Error())
		host.mutex.Unlock()
		return nil, err
	}
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func (host *githubPruningHTTPBoundary) response(request *http.Request) (int, string, error) {
	if request.URL.Host == "api.github.com" {
		return host.githubResponse(request)
	}
	if request.URL.Host == "actions.test" {
		return host.actionsResponse(request)
	}
	return 0, "", fmt.Errorf("unexpected pruning host %q", request.URL.Host)
}

func (host *githubPruningHTTPBoundary) githubResponse(request *http.Request) (int, string, error) {
	switch request.Method + " " + request.URL.Path {
	case "GET /app/installations":
		if host.scenario.discoveryInconclusive {
			return http.StatusOK, `{`, nil
		}
		return http.StatusOK, `[{"id":71}]`, nil
	case "POST /app/installations/71/access_tokens":
		return http.StatusCreated, `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`, nil
	case "GET /installation/repositories":
		return http.StatusOK, `{"total_count":1,"repositories":[{"html_url":"https://github.com/acme/widgets"}]}`, nil
	case "POST /repos/acme/widgets/actions/runners/registration-token":
		return http.StatusCreated, `{"token":"registration-token","expires_at":"2099-01-01T00:00:00Z"}`, nil
	case "POST /actions/runner-registration":
		token, err := productionCompositionAdminToken()
		if err != nil {
			return 0, "", err
		}
		return http.StatusOK, fmt.Sprintf(`{"url":"https://actions.test","token":%q}`, token), nil
	default:
		return 0, "", unexpectedProductionCompositionRequest(request)
	}
}

func (host *githubPruningHTTPBoundary) actionsResponse(request *http.Request) (int, string, error) {
	switch request.Method + " " + request.URL.Path {
	case "GET /_apis/runtime/runnergroups/":
		return http.StatusOK, `{"count":1,"value":[{"id":11,"name":"default","isDefaultGroup":true}]}`, nil
	case "GET /_apis/runtime/runnerscalesets":
		return host.runnerScaleSetsResponse(request)
	case "DELETE /_apis/runtime/runnerscalesets/29":
		return host.deleteRunnerScaleSetResponse(request)
	default:
		return 0, "", unexpectedProductionCompositionRequest(request)
	}
}

func (host *githubPruningHTTPBoundary) deleteRunnerScaleSetResponse(
	request *http.Request,
) (int, string, error) {
	host.mutex.Lock()
	host.deleteAttempts++
	barrier := host.deleteBarrier
	host.deleteBarrier = nil
	host.mutex.Unlock()
	if barrier == nil {
		return host.scenario.deleteStatus, `{}`, nil
	}
	if err := barrier.wait(request.Context()); err != nil {
		return 0, "", fmt.Errorf("wait to complete guarded GitHub delete: %w", err)
	}
	return host.scenario.deleteStatus, `{}`, nil
}

func (host *githubPruningHTTPBoundary) runnerScaleSetsResponse(request *http.Request) (int, string, error) {
	listed := host.scenario.listed
	if name := request.URL.Query().Get("name"); name != "" {
		listed = runnerScaleSetsMatchingName(listed, name)
	}
	encoded, err := json.Marshal(map[string]any{"count": len(listed), "value": listed})
	if err != nil {
		return 0, "", fmt.Errorf("marshal pruning scale sets: %w", err)
	}
	return http.StatusOK, string(encoded), nil
}

func runnerScaleSetsMatchingName(listed []upstream.RunnerScaleSet, name string) []upstream.RunnerScaleSet {
	matching := make([]upstream.RunnerScaleSet, 0, len(listed))
	for _, runnerScaleSet := range listed {
		if runnerScaleSet.Name == name {
			matching = append(matching, runnerScaleSet)
		}
	}
	return matching
}

func (host *githubPruningHTTPBoundary) observations() (int, []string) {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	return host.deleteAttempts, append([]string(nil), host.unexpectedRequest...)
}

var _ http.RoundTripper = (*githubPruningHTTPBoundary)(nil)
