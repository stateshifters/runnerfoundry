//go:build e2e

package daemon

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/provider/contract"
	dockerprovider "github.com/stateshifters/runnerfoundry/internal/provider/docker"
)

type productionCompositionLogDestination struct {
	mutex      sync.Mutex
	content    bytes.Buffer
	reconciled chan struct{}
}

func (destination *productionCompositionLogDestination) Open(config.LogConfig) (io.WriteCloser, error) {
	return destination, nil
}

func (destination *productionCompositionLogDestination) Write(content []byte) (int, error) {
	destination.mutex.Lock()
	_, _ = destination.content.Write(content)
	destination.mutex.Unlock()
	if completedProductionCompositionReconciliation(content) {
		destination.reconciled <- struct{}{}
	}
	return len(content), nil
}

func completedProductionCompositionReconciliation(content []byte) bool {
	textRecord := bytes.Contains(content, []byte("operation=source_reconciliation")) &&
		bytes.Contains(content, []byte("stage=reconciliation")) &&
		bytes.Contains(content, []byte("outcome=completed"))
	jsonRecord := bytes.Contains(content, []byte(`"operation":"source_reconciliation"`)) &&
		bytes.Contains(content, []byte(`"stage":"reconciliation"`)) &&
		bytes.Contains(content, []byte(`"outcome":"completed"`))
	return textRecord || jsonRecord
}

func (*productionCompositionLogDestination) Close() error { return nil }

func (destination *productionCompositionLogDestination) String() string {
	destination.mutex.Lock()
	defer destination.mutex.Unlock()
	return destination.content.String()
}

func (destination *productionCompositionLogDestination) assignedDemandRecovered(runnerPool string) bool {
	destination.mutex.Lock()
	defer destination.mutex.Unlock()
	for _, line := range strings.Split(destination.content.String(), "\n") {
		if strings.Contains(line, "operation=assigned_demand") && strings.Contains(line, "outcome=completed") &&
			strings.Contains(line, "runner_pool="+runnerPool) && strings.Contains(line, "jobs_added=1") {
			return true
		}
	}
	return false
}

type productionCompositionGitHub struct {
	mutex                 sync.Mutex
	messages              map[string]*upstream.RunnerScaleSetMessage
	unexpected            []string
	registrations         chan string
	sessions              chan string
	acknowledged          chan string
	scaleSetPools         map[string]string
	capacities            map[string]productionCompositionCapacity
	capacityTick          uint64
	capacityChanged       chan struct{}
	initialStatistics     map[string]*upstream.RunnerScaleSetStatistic
	omitInitialStatistics map[string]bool
	pollGuards            map[string]func() error
}

type productionCompositionCapacity struct {
	maximum string
	tick    uint64
}

func newProductionCompositionGitHub() *productionCompositionGitHub {
	return &productionCompositionGitHub{
		messages:      make(map[string]*upstream.RunnerScaleSetMessage),
		registrations: make(chan string, 16), sessions: make(chan string, 4), acknowledged: make(chan string, 16),
		scaleSetPools: make(map[string]string), capacities: make(map[string]productionCompositionCapacity),
		capacityChanged:       make(chan struct{}),
		initialStatistics:     make(map[string]*upstream.RunnerScaleSetStatistic),
		omitInitialStatistics: make(map[string]bool),
		pollGuards:            make(map[string]func() error),
	}
}

func (host *productionCompositionGitHub) guardPoll(runnerPool string, guard func() error) {
	host.mutex.Lock()
	host.pollGuards[runnerPool] = guard
	host.mutex.Unlock()
}

func (host *productionCompositionGitHub) setInitialAssignedDemand(runnerPool string, assigned int) {
	host.mutex.Lock()
	host.initialStatistics[runnerPool] = &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: assigned}
	host.mutex.Unlock()
}

func (host *productionCompositionGitHub) omitSessionStatistics(runnerPool string) {
	host.mutex.Lock()
	host.omitInitialStatistics[runnerPool] = true
	host.mutex.Unlock()
}

func (host *productionCompositionGitHub) RoundTrip(request *http.Request) (*http.Response, error) {
	status, body, err := host.response(request)
	if err != nil {
		host.mutex.Lock()
		host.unexpected = append(host.unexpected, err.Error())
		host.mutex.Unlock()
		return nil, err
	}
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func (host *productionCompositionGitHub) response(request *http.Request) (int, string, error) {
	switch request.URL.Host {
	case "api.github.com":
		return host.githubAPIResponse(request)
	case "actions.test":
		return host.actionsServiceResponse(request)
	case "queue.test":
		return host.queueResponse(request)
	default:
		return 0, "", fmt.Errorf("unexpected GitHub host %q", request.URL.Host)
	}
}

func (host *productionCompositionGitHub) githubAPIResponse(request *http.Request) (int, string, error) {
	switch request.Method + " " + request.URL.Path {
	case "GET /app/installations":
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

func (host *productionCompositionGitHub) actionsServiceResponse(request *http.Request) (int, string, error) {
	switch request.Method {
	case http.MethodGet:
		return host.actionsServiceGetResponse(request)
	case http.MethodPost:
		return host.actionsServicePostResponse(request)
	case http.MethodDelete:
		if strings.Contains(request.URL.Path, "/sessions/") {
			return http.StatusNoContent, "", nil
		}
	}
	return 0, "", unexpectedProductionCompositionRequest(request)
}

func (host *productionCompositionGitHub) actionsServiceGetResponse(request *http.Request) (int, string, error) {
	switch request.URL.Path {
	case "/_apis/runtime/runnergroups/":
		if request.URL.Query().Get("groupName") != "default" {
			return 0, "", fmt.Errorf("unexpected runner group query %q", request.URL.RawQuery)
		}
		return http.StatusOK, `{"count":1,"value":[{"id":11,"name":"default","isDefaultGroup":true}]}`, nil
	case "/_apis/runtime/runnerscalesets":
		return host.runnerScaleSetResponse(request)
	case "/_apis/distributedtask/pools/0/agents":
		if request.URL.Query().Get("agentName") == "" || request.URL.Query().Get("api-version") != "6.0-preview" {
			return 0, "", fmt.Errorf("unexpected runner inspection query %q", request.URL.RawQuery)
		}
		return http.StatusOK, `{"count":0,"value":[]}`, nil
	}
	return 0, "", unexpectedProductionCompositionRequest(request)
}

func (host *productionCompositionGitHub) actionsServicePostResponse(request *http.Request) (int, string, error) {
	switch {
	case strings.HasSuffix(request.URL.Path, "/sessions"):
		return host.sessionResponse(request.URL.Path)
	case strings.HasSuffix(request.URL.Path, "/acquirejobs"):
		return acquireJobsResponse(request)
	case strings.HasSuffix(request.URL.Path, "/generatejitconfig"):
		return http.StatusOK, `{"encodedJITConfig":"production-composition-bootstrap"}`, nil
	}
	return 0, "", unexpectedProductionCompositionRequest(request)
}

func (host *productionCompositionGitHub) queueResponse(request *http.Request) (int, string, error) {
	if !strings.HasPrefix(request.URL.Path, "/messages/") {
		return 0, "", unexpectedProductionCompositionRequest(request)
	}
	identifier := strings.TrimPrefix(request.URL.Path, "/messages/")
	switch request.Method {
	case http.MethodGet:
		return host.queueGetResponse(request, identifier)
	case http.MethodDelete:
		host.acknowledged <- request.URL.Path
		return http.StatusNoContent, "", nil
	}
	return 0, "", unexpectedProductionCompositionRequest(request)
}

func (host *productionCompositionGitHub) queueGetResponse(
	request *http.Request,
	identifier string,
) (int, string, error) {
	host.mutex.Lock()
	runnerPool := host.scaleSetPools[identifier]
	guard := host.pollGuards[runnerPool]
	host.mutex.Unlock()
	if guard != nil {
		if err := guard(); err != nil {
			return 0, "", err
		}
	}
	if err := host.recordCapacity(identifier, request.Header.Get(upstream.HeaderScaleSetMaxCapacity)); err != nil {
		return 0, "", err
	}
	return host.messageResponse(identifier)
}

func unexpectedProductionCompositionRequest(request *http.Request) error {
	return fmt.Errorf("unexpected GitHub request %s %s?%s", request.Method, request.URL.Path, request.URL.RawQuery)
}

func (host *productionCompositionGitHub) runnerScaleSetResponse(request *http.Request) (int, string, error) {
	name := request.URL.Query().Get("name")
	identifier := map[string]int{"docker-pool": 29, "libvirt-pool": 30}[name]
	if identifier == 0 || request.URL.Query().Get("runnerGroupId") != "11" {
		return 0, "", fmt.Errorf("unexpected runner scale set query %q", request.URL.RawQuery)
	}
	host.registrations <- name
	host.mutex.Lock()
	host.scaleSetPools[fmt.Sprint(identifier)] = name
	host.mutex.Unlock()
	return http.StatusOK, fmt.Sprintf(`{"count":1,"value":[{"id":%d,"name":%q,"runnerGroupId":11}]}`, identifier, name), nil
}

func (host *productionCompositionGitHub) recordCapacity(identifier, maximum string) error {
	host.mutex.Lock()
	defer host.mutex.Unlock()
	runnerPool := host.scaleSetPools[identifier]
	if runnerPool == "" {
		return fmt.Errorf("capacity observed for unknown production scale set %q", identifier)
	}
	host.capacityTick++
	host.capacities[runnerPool] = productionCompositionCapacity{maximum: maximum, tick: host.capacityTick}
	close(host.capacityChanged)
	host.capacityChanged = make(chan struct{})
	return nil
}

func (host *productionCompositionGitHub) sessionResponse(path string) (int, string, error) {
	switch path {
	case "/_apis/runtime/runnerscalesets/29/sessions":
		return host.sessionForIdentifier("29")
	case "/_apis/runtime/runnerscalesets/30/sessions":
		return host.sessionForIdentifier("30")
	default:
		return 0, "", fmt.Errorf("unexpected session path %q", path)
	}
}

func (host *productionCompositionGitHub) sessionForIdentifier(identifier string) (int, string, error) {
	host.sessions <- identifier
	host.mutex.Lock()
	runnerPool := host.scaleSetPools[identifier]
	statistics := host.initialStatistics[runnerPool]
	omitStatistics := host.omitInitialStatistics[runnerPool]
	host.mutex.Unlock()
	response := map[string]any{
		"sessionId":               "00000000-0000-4000-8000-0000000000" + identifier,
		"messageQueueUrl":         "https://queue.test/messages/" + identifier,
		"messageQueueAccessToken": "queue-token",
	}
	if !omitStatistics {
		if statistics == nil {
			statistics = &upstream.RunnerScaleSetStatistic{}
		}
		response["statistics"] = statistics
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return 0, "", fmt.Errorf("marshal scripted GitHub session: %w", err)
	}
	return http.StatusOK, string(encoded), nil
}

func (host *productionCompositionGitHub) messageResponse(identifier string) (int, string, error) {
	host.mutex.Lock()
	message := host.messages[identifier]
	delete(host.messages, identifier)
	host.mutex.Unlock()
	if message == nil {
		return http.StatusAccepted, "", nil
	}
	batched, err := productionCompositionMessageBatch(message)
	if err != nil {
		return 0, "", err
	}
	response, err := json.Marshal(map[string]any{
		"messageId": message.MessageID, "messageType": "RunnerScaleSetJobMessages",
		"body": string(batched), "statistics": message.Statistics,
	})
	if err != nil {
		return 0, "", fmt.Errorf("marshal scripted GitHub response: %w", err)
	}
	return http.StatusOK, string(response), nil
}

func productionCompositionMessageBatch(message *upstream.RunnerScaleSetMessage) ([]byte, error) {
	batchedMessages := make([]any, 0, len(message.JobAvailableMessages)+len(message.JobAssignedMessages)+
		len(message.JobStartedMessages)+len(message.JobCompletedMessages))
	for _, available := range message.JobAvailableMessages {
		batchedMessages = append(batchedMessages, available)
	}
	for _, assigned := range message.JobAssignedMessages {
		batchedMessages = append(batchedMessages, assigned)
	}
	for _, started := range message.JobStartedMessages {
		batchedMessages = append(batchedMessages, started)
	}
	for _, completed := range message.JobCompletedMessages {
		batchedMessages = append(batchedMessages, completed)
	}
	batched, err := json.Marshal(batchedMessages)
	if err != nil {
		return nil, fmt.Errorf("marshal scripted GitHub message: %w", err)
	}
	return batched, nil
}

func (host *productionCompositionGitHub) queueStarted(
	identifier string,
	messageID int,
	requestID int64,
	runnerName string,
) {
	host.queueMessage(identifier, &upstream.RunnerScaleSetMessage{
		MessageID: messageID, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobStartedMessages: []*upstream.JobStarted{{
			RunnerName: runnerName,
			JobMessageBase: upstream.JobMessageBase{
				JobMessageType:  upstream.JobMessageType{MessageType: upstream.MessageTypeJobStarted},
				RunnerRequestID: requestID,
			},
		}},
	})
}

func (host *productionCompositionGitHub) queueCompleted(
	identifier string,
	messageID int,
	requestID int64,
	runnerName string,
) {
	host.queueMessage(identifier, &upstream.RunnerScaleSetMessage{
		MessageID: messageID, Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: runnerName, Result: "succeeded",
			JobMessageBase: upstream.JobMessageBase{
				JobMessageType:  upstream.JobMessageType{MessageType: upstream.MessageTypeJobCompleted},
				RunnerRequestID: requestID,
			},
		}},
	})
}

func (host *productionCompositionGitHub) queueMessage(
	identifier string,
	message *upstream.RunnerScaleSetMessage,
) {
	host.mutex.Lock()
	host.messages[identifier] = message
	host.mutex.Unlock()
}

func (host *productionCompositionGitHub) queue(identifier string, requestID int64, queuedAt time.Time) {
	host.queueMessage(identifier, &upstream.RunnerScaleSetMessage{
		MessageID: int(requestID), Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			JobMessageType:  upstream.JobMessageType{MessageType: upstream.MessageTypeJobAvailable},
			RunnerRequestID: requestID, QueueTime: queuedAt,
		}}},
	})
}

func (host *productionCompositionGitHub) assertExpected(t *testing.T) {
	t.Helper()
	host.mutex.Lock()
	defer host.mutex.Unlock()
	if len(host.unexpected) != 0 {
		t.Fatalf("unexpected GitHub requests: %v", host.unexpected)
	}
}

func acquireJobsResponse(request *http.Request) (int, string, error) {
	var requestIDs []int64
	if err := json.NewDecoder(request.Body).Decode(&requestIDs); err != nil {
		return 0, "", fmt.Errorf("decode acquire jobs request: %w", err)
	}
	response, err := json.Marshal(map[string]any{"count": len(requestIDs), "value": requestIDs})
	if err != nil {
		return 0, "", fmt.Errorf("marshal acquire jobs response: %w", err)
	}
	return http.StatusOK, string(response), nil
}

func productionCompositionAdminToken() (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	encoded, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		return "", fmt.Errorf("sign production-composition admin token: %w", err)
	}
	return encoded, nil
}

func productionCompositionPrivateKey(t *testing.T) []byte {
	t.Helper()
	primeOne, validOne := new(big.Int).SetString("C49B48187124EE531F26C48AEBB4E3FD4C069D73DD3DA35D0AA87F46834A9BF3F2D3CD90BEDCB8199DBD2552CE670106A95ED0A3E0AE559FA68E846339D542CB", 16)
	primeTwo, validTwo := new(big.Int).SetString("F24E7544C243EC2DC2D3CD84F1B7CB1561FD7A25FE50D139AA338957EC2C5C64D68D8927518766C38CF2A0EA5B17F7D4C784B75B33F0316A0D82248F24533DF7", 16)
	if !validOne || !validTwo {
		t.Fatal("invalid deterministic RSA fixture")
	}
	one := big.NewInt(1)
	modulus := new(big.Int).Mul(primeOne, primeTwo)
	totient := new(big.Int).Mul(new(big.Int).Sub(primeOne, one), new(big.Int).Sub(primeTwo, one))
	privateKey := &rsa.PrivateKey{PublicKey: rsa.PublicKey{N: modulus, E: 65537},
		D: new(big.Int).ModInverse(big.NewInt(65537), totient), Primes: []*big.Int{primeOne, primeTwo}}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
}

type productionCompositionDockerCommands struct {
	mutex          sync.Mutex
	instances      map[string]dockerprovider.StartRequest
	startBehaviors []productionCompositionDockerStartBehavior
	contexts       chan productionCompositionCommandContext
	starts         chan dockerprovider.StartRequest
	removals       chan string
}

type productionCompositionCommandContext struct {
	operation   string
	deadline    time.Time
	observedAt  time.Time
	hasDeadline bool
}

func recordProductionCompositionCommandContext(
	ctx context.Context,
	operation string,
	observations chan<- productionCompositionCommandContext,
) {
	deadline, hasDeadline := ctx.Deadline()
	observations <- productionCompositionCommandContext{
		operation: operation, deadline: deadline, observedAt: time.Now(), hasDeadline: hasDeadline,
	}
}

type productionCompositionDockerStartBehavior struct {
	err     error
	release <-chan struct{}
}

func newProductionCompositionDockerCommands() *productionCompositionDockerCommands {
	return &productionCompositionDockerCommands{
		instances: make(map[string]dockerprovider.StartRequest),
		contexts:  make(chan productionCompositionCommandContext, 256),
		starts:    make(chan dockerprovider.StartRequest, 8), removals: make(chan string, 8),
	}
}

func (commands *productionCompositionDockerCommands) blockNextStartWithPartial(err error) chan<- struct{} {
	release := make(chan struct{})
	commands.mutex.Lock()
	commands.startBehaviors = append(commands.startBehaviors, productionCompositionDockerStartBehavior{
		err: err, release: release,
	})
	commands.mutex.Unlock()
	return release
}

func (commands *productionCompositionDockerCommands) StartComposeRunner(
	ctx context.Context, configured dockerprovider.ComposeConfig, request dockerprovider.StartRequest,
) error {
	recordProductionCompositionCommandContext(ctx, "start", commands.contexts)
	if configured.ComposeFile != "compose.yaml" || configured.Service != "runner" {
		return fmt.Errorf("unexpected Docker Compose config %+v", configured)
	}
	commands.mutex.Lock()
	commands.instances[productionCompositionDockerID(request.RunnerName)] = request
	var behavior productionCompositionDockerStartBehavior
	if len(commands.startBehaviors) != 0 {
		behavior = commands.startBehaviors[0]
		commands.startBehaviors = commands.startBehaviors[1:]
	}
	commands.mutex.Unlock()
	commands.starts <- request
	if behavior.release != nil {
		select {
		case <-behavior.release:
		case <-ctx.Done():
			return errors.Join(behavior.err, ctx.Err())
		}
	}
	return behavior.err
}

func (commands *productionCompositionDockerCommands) ListComposeRunners(
	ctx context.Context,
	_ string,
) (string, error) {
	recordProductionCompositionCommandContext(ctx, "inventory", commands.contexts)
	commands.mutex.Lock()
	containers := make([]map[string]string, 0, len(commands.instances))
	for identifier, request := range commands.instances {
		containers = append(containers, map[string]string{
			"ID": identifier, "Names": request.RunnerName, "State": "running",
			"Labels": contract.OwnershipRunnerPool + "=" + request.RunnerPoolName + "," +
				contract.OwnershipRunner + "=" + request.RunnerName,
		})
	}
	commands.mutex.Unlock()
	encoded, err := json.Marshal(containers)
	if err != nil {
		return "", fmt.Errorf("marshal scripted Docker inventory: %w", err)
	}
	return string(encoded), nil
}

func (commands *productionCompositionDockerCommands) RemoveComposeRunner(ctx context.Context, identifier string) error {
	recordProductionCompositionCommandContext(ctx, "destroy", commands.contexts)
	commands.mutex.Lock()
	delete(commands.instances, identifier)
	commands.mutex.Unlock()
	commands.removals <- identifier
	return nil
}
func (commands *productionCompositionDockerCommands) ListComposeServices(
	ctx context.Context, _ dockerprovider.ComposeConfig,
) (string, error) {
	recordProductionCompositionCommandContext(ctx, "validate", commands.contexts)
	return "runner\n", nil
}

func productionCompositionDockerID(runnerName string) string { return "container-" + runnerName }

type productionCompositionLibvirtCommands struct {
	mutex    sync.Mutex
	domains  map[string]string
	contexts chan productionCompositionCommandContext
	starts   chan string
	destroys chan string
}

func newProductionCompositionLibvirtCommands() *productionCompositionLibvirtCommands {
	return &productionCompositionLibvirtCommands{
		domains: make(map[string]string), contexts: make(chan productionCompositionCommandContext, 256),
		starts: make(chan string, 8), destroys: make(chan string, 8),
	}
}

func (commands *productionCompositionLibvirtCommands) CreateDomain(ctx context.Context, uri, domainXML string) error {
	recordProductionCompositionCommandContext(ctx, "start", commands.contexts)
	if uri != "qemu:///system" {
		return fmt.Errorf("unexpected libvirt URI %q", uri)
	}
	domainName, err := productionCompositionDomainName(domainXML)
	if err != nil {
		return err
	}
	commands.mutex.Lock()
	commands.domains[domainName] = domainXML
	commands.mutex.Unlock()
	commands.starts <- domainXML
	return nil
}

func (commands *productionCompositionLibvirtCommands) ListDomains(ctx context.Context, _ string) (string, error) {
	recordProductionCompositionCommandContext(ctx, "inventory", commands.contexts)
	commands.mutex.Lock()
	names := make([]string, 0, len(commands.domains))
	for name := range commands.domains {
		names = append(names, name)
	}
	commands.mutex.Unlock()
	return strings.Join(names, "\n"), nil
}

func (commands *productionCompositionLibvirtCommands) ReadDomainXML(
	ctx context.Context,
	_, domainName string,
) (string, error) {
	recordProductionCompositionCommandContext(ctx, "inventory", commands.contexts)
	commands.mutex.Lock()
	domainXML, exists := commands.domains[domainName]
	commands.mutex.Unlock()
	if !exists {
		return "", fmt.Errorf("scripted libvirt domain %q is absent", domainName)
	}
	return domainXML, nil
}

func (commands *productionCompositionLibvirtCommands) ReadDomainState(
	ctx context.Context,
	_, _ string,
) (string, error) {
	recordProductionCompositionCommandContext(ctx, "inventory", commands.contexts)
	return "running", nil
}

func (commands *productionCompositionLibvirtCommands) DestroyDomain(
	ctx context.Context,
	_, domainName string,
) error {
	recordProductionCompositionCommandContext(ctx, "destroy", commands.contexts)
	commands.mutex.Lock()
	delete(commands.domains, domainName)
	commands.mutex.Unlock()
	commands.destroys <- domainName
	return nil
}
func (commands *productionCompositionLibvirtCommands) ReadConnectionURI(
	ctx context.Context,
	uri string,
) (string, error) {
	recordProductionCompositionCommandContext(ctx, "validate", commands.contexts)
	return uri, nil
}

func productionCompositionDomainName(domainXML string) (string, error) {
	var domain struct {
		Name string `xml:"name"`
	}
	if err := xml.Unmarshal([]byte(domainXML), &domain); err != nil {
		return "", fmt.Errorf("decode scripted libvirt domain: %w", err)
	}
	if strings.TrimSpace(domain.Name) == "" {
		return "", errors.New("scripted libvirt domain has no name")
	}
	return domain.Name, nil
}
