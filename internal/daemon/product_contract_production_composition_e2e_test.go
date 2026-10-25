//go:build e2e

package daemon

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
)

func TestProductContractProductionCompositionReachesDurableLifecycle(t *testing.T) {
	state, enabled := productionCompositionState(t)
	if !enabled {
		return
	}
	configured := loadProductionCompositionConfig(t)
	configured.State = state
	host := newProductionCompositionGitHub()
	t.Cleanup(func() { host.assertExpected(t) })
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	runtime, generation, tickers := openProductionComposition(t, configured, host, dockerCommands, libvirtCommands)
	defer closeProductionComposition(context.Background(), t, runtime, generation)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("production-composition logs:\n%s", tickers.logs.String())
		}
	})
	exerciseProductionCompositionLifecycle(t, runtime, tickers, host, dockerCommands, libvirtCommands)
}

func productionCompositionState(t *testing.T) (config.StateConfig, bool) {
	t.Helper()
	if e2eBackendEnabled("sqlite") {
		return config.StateConfig{Path: ":memory:"}, true
	}
	if e2eBackendEnabled("postgres") {
		fixture := e2ePostgresStart(t)
		settings := fixture.Settings()
		return config.StateConfig{Postgres: &settings}, true
	}
	return config.StateConfig{}, false
}

func exerciseProductionCompositionLifecycle(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
) {
	t.Helper()
	establishProductionCompositionSessions(t, tickers, host)
	if tickers.notificationReady != nil {
		awaitProductionCompositionSignal(t, "production PostgreSQL queue listener readiness", tickers.notificationReady)
	}
	demandCapacity := productionCompositionCapacityCheckpoint(host)
	host.queue("29", 101, time.Date(2020, time.August, 16, 10, 0, 0, 0, time.UTC))
	host.queue("30", 102, time.Date(2020, time.August, 16, 10, 1, 0, 0, time.UTC))
	tickers.complete(t, tickers.source)
	awaitProductionCompositionValues(t, "production GitHub acknowledgements", host.acknowledged, 2)
	tickers.complete(t, tickers.source)

	starts := assertProductionCompositionStarts(t, dockerCommands, libvirtCommands)
	assertProductionCompositionProcessing(t, runtime, "docker-pool")
	assertProductionCompositionProcessing(t, runtime, "libvirt-pool")
	tickers.complete(t, tickers.source)
	awaitProductionCompositionCapacities(t, host, "0", demandCapacity)
	completeProductionCompositionJobs(t, runtime, tickers, host, dockerCommands, libvirtCommands, starts)
	host.assertExpected(t)
}

func establishProductionCompositionSessions(
	t *testing.T,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
) {
	t.Helper()
	routes := []string{"docker-pool", "libvirt-pool"}
	activationCheckpoint := tickers.logs.sessionActivationCheckpoint()
	sessionTargets := productionCompositionRouteTargets(routes, host.sessionObservation)
	e2eAwaitRouteTargets(t, "production GitHub registrations",
		tickers.registrationTargets, host.registrationObservation)
	awaitProductionCompositionRoutes(t, "production GitHub sessions",
		sessionTargets, host.sessionObservation,
		func() { tickers.complete(t, tickers.source) })
	awaitProductionCompositionSessionActivation(t, tickers, activationCheckpoint)
	for _, runnerPool := range []core.RunnerPool{"docker-pool", "libvirt-pool"} {
		tickers.pollCheckpoints[runnerPool], _ = host.pollObservation(string(runnerPool))
	}
}

func productionCompositionRouteTargets(
	routes []string,
	observation func(string) (uint64, <-chan struct{}),
) map[string]uint64 {
	targets := make(map[string]uint64, len(routes))
	for _, route := range routes {
		sequence, _ := observation(route)
		targets[route] = sequence + 1
	}
	return targets
}

type productionCompositionStarts struct {
	dockerRunner  string
	libvirtRunner string
}

func assertProductionCompositionStarts(
	t *testing.T,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
) productionCompositionStarts {
	t.Helper()
	dockerStart := awaitDockerCompositionStart(t, dockerCommands.starts)
	if dockerStart.RunnerPoolName != "docker-pool" || dockerStart.JITConfig != "production-composition-bootstrap" {
		t.Fatalf("production Docker start = %+v", dockerStart)
	}
	libvirtXML := awaitProductionCompositionValue(t, "production libvirt start", libvirtCommands.starts)
	for _, required := range []string{"libvirt-pool", "production-composition-bootstrap", "runnerfoundry"} {
		if !strings.Contains(libvirtXML, required) {
			t.Fatalf("production libvirt XML omitted %q: %s", required, libvirtXML)
		}
	}
	libvirtRunner, err := productionCompositionDomainName(libvirtXML)
	if err != nil {
		t.Fatalf("production libvirt runner name: %v", err)
	}
	return productionCompositionStarts{dockerRunner: dockerStart.RunnerName, libvirtRunner: libvirtRunner}
}

func completeProductionCompositionJobs(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	starts productionCompositionStarts,
) {
	t.Helper()
	markProductionCompositionJobsStarted(t, runtime, tickers, host, starts)
	terminalizeProductionCompositionJobs(t, runtime, tickers, host, starts)
	cleanupProductionCompositionJobs(t, runtime, tickers, host, dockerCommands, libvirtCommands, starts)
}

func markProductionCompositionJobsStarted(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	starts productionCompositionStarts,
) {
	t.Helper()
	host.queueStarted("29", 1101, 101, starts.dockerRunner)
	host.queueStarted("30", 1102, 102, starts.libvirtRunner)
	tickers.complete(t, tickers.source)
	awaitProductionCompositionAcknowledgementsWhileCycling(t, "production started acknowledgements",
		host.acknowledged, tickers, 2)
	tickers.complete(t, tickers.source)
	assertProductionCompositionObservation(t, runtime, "docker-pool", productionCompositionObservation{runnerStarted: true})
	assertProductionCompositionObservation(t, runtime, "libvirt-pool", productionCompositionObservation{runnerStarted: true})
	awaitProductionCompositionCapacities(t, host, "0", nil)
}

func terminalizeProductionCompositionJobs(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	starts productionCompositionStarts,
) {
	t.Helper()
	host.queueCompleted("29", 2101, 101, starts.dockerRunner)
	host.queueCompleted("30", 2102, 102, starts.libvirtRunner)
	tickers.complete(t, tickers.source)
	awaitProductionCompositionAcknowledgementsWhileCycling(t, "production completion acknowledgements",
		host.acknowledged, tickers, 2)
	tickers.complete(t, tickers.source)
	wantTerminal := productionCompositionObservation{runnerStarted: true, sourceTerminal: true}
	assertProductionCompositionObservation(t, runtime, "docker-pool", wantTerminal)
	assertProductionCompositionObservation(t, runtime, "libvirt-pool", wantTerminal)
	awaitProductionCompositionCapacities(t, host, "0", nil)
}

func cleanupProductionCompositionJobs(
	t *testing.T,
	runtime *productionRuntime,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	starts productionCompositionStarts,
) {
	t.Helper()
	tickers.complete(t, tickers.execution)
	dockerRemoval := awaitProductionCompositionValue(t, "terminal Docker cleanup", dockerCommands.removals)
	if dockerRemoval != productionCompositionDockerID(starts.dockerRunner) {
		t.Fatalf("terminal Docker cleanup = %q, want runner %q", dockerRemoval, starts.dockerRunner)
	}
	libvirtDestroy := awaitProductionCompositionValue(t, "terminal libvirt cleanup", libvirtCommands.destroys)
	if libvirtDestroy != starts.libvirtRunner {
		t.Fatalf("terminal libvirt cleanup = %q, want %q", libvirtDestroy, starts.libvirtRunner)
	}
	tickers.complete(t, tickers.execution)
	assertNoProductionCompositionProcessing(t, runtime, "docker-pool")
	assertNoProductionCompositionProcessing(t, runtime, "libvirt-pool")
	restoration := productionCompositionCapacityCheckpoint(host)
	tickers.complete(t, tickers.source)
	awaitProductionCompositionCapacities(t, host, "1", restoration)
}

type productionCompositionObservation struct {
	runnerStarted  bool
	sourceTerminal bool
}

type productionCompositionTickers struct {
	membership          *e2eManualTicker
	source              *e2eManualTicker
	execution           *e2eManualTicker
	barriers            map[*e2eManualTicker]*e2eLaneBarrier
	logs                *productionCompositionLogDestination
	notificationReady   <-chan struct{}
	awaitPublishedPolls func(*testing.T, ...core.RunnerPool)
	pollCheckpoints     map[core.RunnerPool]uint64
	registrationTargets map[string]uint64
}

func openProductionComposition(
	t *testing.T,
	configured config.Config,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	configure ...func(*productionDependencies),
) (*productionRuntime, *runtimeGeneration, productionCompositionTickers) {
	t.Helper()
	tickers := newProductionCompositionTickers(t, host)
	dependencies := productionCompositionDependencies(t, host, dockerCommands, libvirtCommands, &tickers)
	if configured.State.Postgres != nil {
		configureProductionCompositionQueueNotificationReadiness(&dependencies, &tickers)
	}
	for _, customize := range configure {
		customize(&dependencies)
	}
	ctx := context.Background()
	runtime, err := openProductionRuntimeWithDependencies(ctx, configured, dependencies)
	if err != nil {
		t.Fatalf("open production-composition runtime: %v", err)
	}
	generation := buildProductionCompositionGeneration(ctx, t, runtime, configured, &tickers)
	return runtime, generation, tickers
}

func newProductionCompositionTickers(t *testing.T, host *productionCompositionGitHub) productionCompositionTickers {
	t.Helper()
	tickers := productionCompositionTickers{
		membership: newE2EManualTicker(), source: newE2EManualTicker(), execution: newE2EManualTicker(),
		barriers: make(map[*e2eManualTicker]*e2eLaneBarrier), pollCheckpoints: make(map[core.RunnerPool]uint64),
		registrationTargets: productionCompositionRouteTargets(
			[]string{"docker-pool", "libvirt-pool"}, host.registrationObservation,
		),
	}
	for _, ticker := range []*e2eManualTicker{tickers.membership, tickers.source, tickers.execution} {
		tickers.barriers[ticker] = newE2ELaneBarrier()
	}
	tickers.awaitPublishedPolls = func(t *testing.T, runnerPools ...core.RunnerPool) {
		t.Helper()
		awaitProductionCompositionPollHarvest(t, tickers, host, runnerPools)
	}
	return tickers
}

func productionCompositionDependencies(
	t *testing.T,
	host *productionCompositionGitHub,
	dockerCommands *productionCompositionDockerCommands,
	libvirtCommands *productionCompositionLibvirtCommands,
	tickers *productionCompositionTickers,
) productionDependencies {
	t.Helper()
	dependencies := newProductionDependencies()
	dependencies.httpTransport = productionCompositionHTTPTransport(t, host)
	dependencies.scaleSetTransportFactory = func() *http.Transport {
		return productionCompositionHTTPTransport(t, host)
	}
	dependencies.newDockerLauncher = func(
		configured dockerprovider.ComposeConfig,
		memberID core.MemberID,
	) execution.RunnerLauncher {
		return dockerCommands.newLauncher(configured, memberID)
	}
	dependencies.newDockerInventory = func(memberID core.MemberID) execution.RunnerInventory {
		return dockerCommands.newInventory(memberID)
	}
	dependencies.libvirtCommands = libvirtCommands
	dependencies.readHostname = func() (string, error) { return "production-member", nil }
	configureProductionCompositionClockAndLogging(&dependencies, tickers)
	dependencies.readFile = productionCompositionFileReader(t)
	configureProductionCompositionMaintenanceObservation(t, &dependencies, tickers)
	return dependencies
}

func configureProductionCompositionClockAndLogging(
	dependencies *productionDependencies,
	tickers *productionCompositionTickers,
) {
	baseTime := time.Date(2020, time.August, 16, 12, 0, 0, 0, time.UTC)
	dependencies.currentTime = func() time.Time { return baseTime }
	logDestination := &productionCompositionLogDestination{sessionActivationChanged: make(chan struct{})}
	dependencies.logDestinationFactory = logDestination
	tickers.logs = logDestination
}

func configureProductionCompositionMaintenanceObservation(
	t *testing.T,
	dependencies *productionDependencies,
	tickers *productionCompositionTickers,
) {
	t.Helper()
	orderedTickers := []*e2eManualTicker{tickers.membership, tickers.source, tickers.execution}
	nextTicker := 0
	dependencies.newTicker = func(time.Duration) cycleTicker {
		ticker := orderedTickers[nextTicker]
		nextTicker++
		return ticker
	}
	dependencies.maintenanceLaneCompleted = func(_ string, observed cycleTicker) {
		ticker, validTicker := observed.(*e2eManualTicker)
		if !validTicker {
			t.Errorf("completed production-composition ticker = %T, want *e2eManualTicker", observed)
			return
		}
		barrier := tickers.barriers[ticker]
		if barrier == nil {
			t.Error("completed an unknown production-composition ticker")
			return
		}
		barrier.complete()
	}
}

func productionCompositionHTTPTransport(t *testing.T, host *productionCompositionGitHub) *http.Transport {
	t.Helper()
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("default HTTP transport = %T, want *http.Transport", http.DefaultTransport)
	}
	httpTransport := defaultTransport.Clone()
	httpTransport.Proxy = nil
	httpTransport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
		return nil, fmt.Errorf("production-composition HTTP transport forbids %s dial to %s", network, address)
	}
	httpTransport.DialTLSContext = func(_ context.Context, network, address string) (net.Conn, error) {
		return nil, fmt.Errorf("production-composition HTTP transport forbids TLS %s dial to %s", network, address)
	}
	httpTransport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	httpTransport.Protocols = new(http.Protocols)
	httpTransport.ForceAttemptHTTP2 = false
	httpTransport.RegisterProtocol("https", host)
	return httpTransport
}

func productionCompositionFileReader(t *testing.T) config.ReadFile {
	t.Helper()
	privateKey := productionCompositionPrivateKey(t)
	return func(path string) ([]byte, error) {
		if path == "github.pem" {
			return privateKey, nil
		}
		if path == "domain.xml" {
			return []byte(`<domain><name>template</name></domain>`), nil
		}
		return nil, fmt.Errorf("unexpected production composition file %q", path)
	}
}

func buildProductionCompositionGeneration(
	ctx context.Context,
	t *testing.T,
	runtime *productionRuntime,
	configured config.Config,
	tickers *productionCompositionTickers,
) *runtimeGeneration {
	t.Helper()
	generation, err := runtime.Build(ctx, configured)
	if err != nil {
		if closeErr := runtime.Close(ctx); closeErr != nil {
			t.Errorf("close failed production-composition runtime: %v", closeErr)
		}
		t.Fatalf("build production-composition generation: %v", err)
	}
	if err = generation.Prepare(ctx); err != nil {
		closeProductionComposition(ctx, t, runtime, generation)
		t.Fatalf("prepare production-composition generation: %v", err)
	}
	if err = generation.Activate(ctx); err != nil {
		closeProductionComposition(ctx, t, runtime, generation)
		t.Fatalf("activate production-composition generation: %v", err)
	}
	for _, ticker := range []*e2eManualTicker{tickers.membership, tickers.source, tickers.execution} {
		tickers.barriers[ticker].waitAfter(t, "initial production-composition lane", 0)
	}
	return generation
}

func (tickers productionCompositionTickers) complete(t *testing.T, ticker *e2eManualTicker) {
	t.Helper()
	barrier := tickers.barriers[ticker]
	if barrier == nil {
		t.Fatal("production-composition ticker has no completion barrier")
	}
	before := barrier.count()
	ticker.Tick(time.Now())
	barrier.waitAfter(t, "production-composition maintenance lane", before)
}

func awaitProductionCompositionRoutes(
	t *testing.T,
	description string,
	targets map[string]uint64,
	observation func(string) (uint64, <-chan struct{}),
	cycle func(),
) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		pending := make([]string, 0, len(targets))
		for route, target := range targets {
			sequence, _ := observation(route)
			if sequence < target {
				pending = append(pending, route)
			}
		}
		if len(pending) == 0 {
			return
		}
		cycle()
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for %s; pending routes %v", description, pending)
		default:
		}
	}
}

func awaitProductionCompositionSessionActivation(
	t *testing.T,
	tickers productionCompositionTickers,
	checkpoint uint64,
) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		completed, _ := tickers.logs.sessionActivationAfter(checkpoint)
		if completed {
			return
		}
		tickers.complete(t, tickers.source)
		select {
		case <-timer.C:
			t.Fatal("timed out waiting for production GitHub session activation")
		default:
		}
	}
}

func awaitProductionCompositionPollHarvest(
	t *testing.T,
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	runnerPools []core.RunnerPool,
) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	targets, completed := productionCompositionPollTargets(tickers, host, runnerPools)
	for {
		pending := pendingProductionCompositionPolls(tickers, host, runnerPools, targets, completed)
		if len(pending) == 0 {
			tickers.complete(t, tickers.source)
			return
		}
		tickers.complete(t, tickers.source)
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for production GitHub poll harvest; pending Runner Pools %v", pending)
		default:
		}
	}
}

func productionCompositionPollTargets(
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	runnerPools []core.RunnerPool,
) (map[core.RunnerPool]uint64, map[core.RunnerPool]bool) {
	targets := make(map[core.RunnerPool]uint64, len(runnerPools))
	completed := make(map[core.RunnerPool]bool, len(runnerPools))
	for _, runnerPool := range runnerPools {
		if host.omitsInitialStatistics(string(runnerPool)) {
			completed[runnerPool] = true
			continue
		}
		targets[runnerPool] = tickers.pollCheckpoints[runnerPool] + 2
	}
	return targets, completed
}

func pendingProductionCompositionPolls(
	tickers productionCompositionTickers,
	host *productionCompositionGitHub,
	runnerPools []core.RunnerPool,
	targets map[core.RunnerPool]uint64,
	completed map[core.RunnerPool]bool,
) []core.RunnerPool {
	pending := make([]core.RunnerPool, 0, len(runnerPools))
	for _, runnerPool := range runnerPools {
		if completed[runnerPool] {
			continue
		}
		sequence, _ := host.pollObservation(string(runnerPool))
		if sequence < targets[runnerPool] {
			pending = append(pending, runnerPool)
			continue
		}
		completed[runnerPool] = true
		tickers.pollCheckpoints[runnerPool] = sequence
	}
	return pending
}

func loadProductionCompositionConfig(t *testing.T) config.Config {
	t.Helper()
	document := `github:
  apps:
    - app_id: 12345
      private_key_file: github.pem
  discovery_interval: 5m
state:
  path: ":memory:"
scale_sets:
  - name: docker-pool
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
  - name: libvirt-pool
    max_runners: 1
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: domain.xml
`
	configured, err := config.Load(strings.NewReader(document))
	if err != nil {
		t.Fatalf("load production-composition config: %v", err)
	}
	return configured
}

func assertProductionCompositionProcessing(
	t *testing.T,
	runtime *productionRuntime,
	runnerPool core.RunnerPool,
) core.ProcessingJob {
	t.Helper()
	records, err := runtime.state.store.processing.ProcessingByMember(context.Background(), runtime.state.member, runnerPool)
	if err != nil || len(records) != 1 || records[0].Job.RunnerPool != runnerPool || records[0].InventoryEndpointID == "" {
		t.Fatalf("production processing for %q = (%s, %v)", runnerPool, describeProcessing(records), err)
	}
	return records[0]
}

func assertProductionCompositionObservation(
	t *testing.T,
	runtime *productionRuntime,
	runnerPool core.RunnerPool,
	want productionCompositionObservation,
) {
	t.Helper()
	processing := assertProductionCompositionProcessing(t, runtime, runnerPool)
	if processing.RunnerAccepted != want.runnerStarted || processing.SourceTerminal != want.sourceTerminal {
		t.Fatalf("production processing observation for %q = started:%t terminal:%t, want started:%t terminal:%t",
			runnerPool, processing.RunnerAccepted, processing.SourceTerminal, want.runnerStarted, want.sourceTerminal)
	}
}

func assertNoProductionCompositionProcessing(
	t *testing.T,
	runtime *productionRuntime,
	runnerPool core.RunnerPool,
) {
	t.Helper()
	records, err := runtime.state.store.processing.ProcessingByMember(context.Background(), runtime.state.member, runnerPool)
	if err != nil || len(records) != 0 {
		t.Fatalf("production processing after cleanup for %q = (%s, %v)", runnerPool, describeProcessing(records), err)
	}
}

func closeProductionComposition(
	parent context.Context,
	t *testing.T,
	runtime *productionRuntime,
	generation *runtimeGeneration,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, e2eWaitLimit)
	defer cancel()
	if err := generation.Close(ctx); err != nil {
		t.Errorf("close production-composition generation: %v", err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Errorf("close production-composition runtime: %v", err)
	}
}

func awaitProductionCompositionValues(t *testing.T, description string, values <-chan string, count int) {
	t.Helper()
	for index := 0; index < count; index++ {
		_ = awaitProductionCompositionValue(t, description, values)
	}
}

func awaitProductionCompositionValue(t *testing.T, description string, values <-chan string) string {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		return ""
	}
}

func awaitProductionCompositionSignal(t *testing.T, description string, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitProductionCompositionCapacities(
	t *testing.T,
	host *productionCompositionGitHub,
	maximum string,
	after map[string]uint64,
) {
	t.Helper()
	timer := time.NewTimer(e2eWaitLimit)
	defer timer.Stop()
	for {
		pending, changed := pendingProductionCompositionCapacities(host, maximum, after)
		if len(pending) == 0 {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for production GitHub capacity %s by Runner Pool; pending %v", maximum, pending)
		}
	}
}

func pendingProductionCompositionCapacities(
	host *productionCompositionGitHub,
	maximum string,
	after map[string]uint64,
) (map[string]productionCompositionCapacity, <-chan struct{}) {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	pending := make(map[string]productionCompositionCapacity, 2)
	for _, runnerPool := range []string{"docker-pool", "libvirt-pool"} {
		if !productionCompositionCapacityObserved(host.capacityHistory[runnerPool], maximum, after[runnerPool]) {
			pending[runnerPool] = host.capacities[runnerPool]
		}
	}
	return pending, host.capacityChanged
}

func productionCompositionCapacityObserved(
	history []productionCompositionCapacity,
	maximum string,
	after uint64,
) bool {
	for _, observation := range history {
		if observation.tick > after && observation.maximum == maximum {
			return true
		}
	}
	return false
}

func productionCompositionCapacityCheckpoint(host *productionCompositionGitHub) map[string]uint64 {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	checkpoint := make(map[string]uint64, len(host.capacities))
	for runnerPool, observation := range host.capacities {
		checkpoint[runnerPool] = observation.tick
	}
	return checkpoint
}
