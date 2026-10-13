package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

func TestClientKeepsRepositoryAuthenticationAndSessionHTTPIndependent(t *testing.T) {
	t.Parallel()
	boundary := &ownershipHTTPBoundary{pollStarted: make(chan struct{}), releasePoll: make(chan struct{})}
	client := newOwnershipClient(t, boundary)
	session := openOwnershipSession(t, client, 29)
	pollResult := make(chan error, 1)
	go func() { _, pollErr := session.GetMessage(t.Context(), 0, 1); pollResult <- pollErr }()
	<-boundary.pollStarted
	group, err := client.ResolveRunnerGroup(t.Context(), "default")
	if err != nil || group != 17 {
		t.Errorf("repository request during poll = (%d, %v)", group, err)
	}
	sibling := openOwnershipSession(t, client, 30)
	close(boundary.releasePoll)
	if err := <-pollResult; !errors.Is(err, ErrNoMessage) {
		t.Errorf("empty poll = %v", err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Errorf("close session: %v", err)
	}
	if err := sibling.Close(t.Context()); err != nil {
		t.Errorf("close sibling: %v", err)
	}
	assertHTTPOwnerIsolation(t, boundary)
}

func newOwnershipClient(t *testing.T, boundary *ownershipHTTPBoundary) *Client {
	t.Helper()
	client, err := NewClient(Repository{InstallationID: 11, URL: "https://github.com/acme/widgets"}, deterministicCredentials(t), boundary.newTransport, false)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return client
}

func openOwnershipSession(t *testing.T, client *Client, identifier ScaleSetID) *Session {
	t.Helper()
	session, err := client.OpenSession(t.Context(), Registration{ID: identifier}, "member")
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	return session
}

func assertHTTPOwnerIsolation(t *testing.T, boundary *ownershipHTTPBoundary) {
	t.Helper()
	boundary.mutex.Lock()
	defer boundary.mutex.Unlock()
	if boundary.transports != 4 {
		t.Fatalf("HTTP owners = %d, want repository, authentication, and two sessions", boundary.transports)
	}
	for owner, requests := range boundary.requests {
		assertOwnerRequests(t, owner, requests)
	}
}

func assertOwnerRequests(t *testing.T, owner int, requests []string) {
	t.Helper()
	for _, path := range requests {
		if owner < 2 && (strings.Contains(path, "/sessions") || path == "/messages") {
			t.Errorf("parent owner %d sent session request %s", owner, path)
		}
		if owner >= 2 && strings.Contains(path, "runner-registration") {
			t.Errorf("session owner %d sent parent authentication request", owner)
		}
	}
}

type ownershipHTTPBoundary struct {
	mutex       sync.Mutex
	transports  int
	requests    map[int][]string
	pollStarted chan struct{}
	releasePoll chan struct{}
}

func (boundary *ownershipHTTPBoundary) newTransport() *http.Transport {
	boundary.mutex.Lock()
	owner := boundary.transports
	boundary.transports++
	boundary.mutex.Unlock()
	transport := &http.Transport{DialContext: rejectHTTPDial, DialTLSContext: rejectHTTPDial}
	transport.RegisterProtocol("https", ownershipHTTPRequest{boundary: boundary, owner: owner})
	return transport
}

func rejectHTTPDial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("network dialing is forbidden")
}

type ownershipHTTPRequest struct {
	boundary *ownershipHTTPBoundary
	owner    int
}

func (requester ownershipHTTPRequest) RoundTrip(request *http.Request) (*http.Response, error) {
	boundary := requester.boundary
	boundary.mutex.Lock()
	if boundary.requests == nil {
		boundary.requests = make(map[int][]string)
	}
	boundary.requests[requester.owner] = append(boundary.requests[requester.owner], request.URL.Path)
	boundary.mutex.Unlock()
	status, body, err := boundary.respond(request)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (boundary *ownershipHTTPBoundary) respond(request *http.Request) (int, string, error) {
	switch request.Method + " " + request.URL.Host + request.URL.Path {
	case "POST api.github.com/app/installations/11/access_tokens":
		return http.StatusCreated, `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`, nil
	case "POST api.github.com/repos/acme/widgets/actions/runners/registration-token":
		return http.StatusCreated, `{"token":"registration-token","expires_at":"2099-01-01T00:00:00Z"}`, nil
	case "POST api.github.com/actions/runner-registration":
		token := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))})
		encoded, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
		return http.StatusOK, fmt.Sprintf(`{"url":"https://actions.test","token":%q}`, encoded), err
	case "GET actions.test/_apis/runtime/runnergroups/":
		return http.StatusOK, `{"count":1,"value":[{"id":17,"name":"default"}]}`, nil
	case "POST actions.test/_apis/runtime/runnerscalesets/29/sessions", "POST actions.test/_apis/runtime/runnerscalesets/30/sessions":
		return http.StatusOK, `{"sessionId":"1d2da680-4390-4c6f-9f04-2f12c518ac34","messageQueueUrl":"https://actions.test/messages","messageQueueAccessToken":"queue-token","statistics":{"totalAssignedJobs":3}}`, nil
	case "GET actions.test/messages":
		close(boundary.pollStarted)
		select {
		case <-boundary.releasePoll:
		case <-request.Context().Done():
			return 0, "", fmt.Errorf("blocked poll canceled: %w", request.Context().Err())
		}
		return http.StatusAccepted, "", nil
	case "DELETE actions.test/_apis/runtime/runnerscalesets/29/sessions/1d2da680-4390-4c6f-9f04-2f12c518ac34", "DELETE actions.test/_apis/runtime/runnerscalesets/30/sessions/1d2da680-4390-4c6f-9f04-2f12c518ac34":
		return http.StatusNoContent, "", nil
	default:
		return 0, "", fmt.Errorf("unexpected scripted request: %s %s", request.Method, request.URL)
	}
}

func TestClientRejectsMissingFactoryTransports(t *testing.T) {
	t.Parallel()
	for _, failingOwner := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("owner_%d", failingOwner), func(t *testing.T) {
			exerciseMissingTransport(t, failingOwner)
		})
	}
}

func exerciseMissingTransport(t *testing.T, failingOwner int) {
	t.Helper()
	calls := 0
	factory := func() *http.Transport {
		calls++
		if calls == failingOwner+1 {
			return nil
		}
		return &http.Transport{DialContext: rejectHTTPDial, DialTLSContext: rejectHTTPDial}
	}
	client, err := NewClient(Repository{InstallationID: 11, URL: "https://github.com/acme/widgets"}, deterministicCredentials(t), factory, false)
	if failingOwner < 2 {
		if err == nil {
			t.Fatal("nil parent transport accepted")
		}
		return
	}
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if _, err := client.OpenSession(t.Context(), Registration{ID: 29}, "member"); err == nil {
		t.Fatal("nil session transport accepted")
	}
}

func TestClientPreservesConfiguredIdleRetentionAndBoundsUnsetRetention(t *testing.T) {
	t.Parallel()
	for _, configured := range []time.Duration{0, 10 * time.Second} {
		var transports []*http.Transport
		factory := func() *http.Transport {
			transport := &http.Transport{IdleConnTimeout: configured, DialContext: rejectHTTPDial, DialTLSContext: rejectHTTPDial}
			transports = append(transports, transport)
			return transport
		}
		_, err := NewClient(Repository{InstallationID: 11, URL: "https://github.com/acme/widgets"}, deterministicCredentials(t), factory, false)
		if err != nil {
			t.Fatalf("create client: %v", err)
		}
		assertIdleRetention(t, transports, configured)
	}
}

func assertIdleRetention(t *testing.T, transports []*http.Transport, configured time.Duration) {
	t.Helper()
	for _, transport := range transports {
		if transport.IdleConnTimeout <= 0 || (configured > 0 && transport.IdleConnTimeout != configured) {
			t.Fatalf("idle retention = %v, configured %v", transport.IdleConnTimeout, configured)
		}
	}
}
