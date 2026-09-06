package transport

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	githubapi "github.com/google/go-github/v88/github"
)

func TestDiscovererAuthenticatesEveryRequestThroughSuppliedTransport(t *testing.T) {
	t.Parallel()
	boundary := &protocolTransport{responses: map[string]protocolResponse{
		"/app/installations":                  {status: http.StatusOK, body: `[{"id":11}]`},
		"/app/installations/11/access_tokens": {status: http.StatusCreated, body: `{"token":"scripted-installation-token","expires_at":"2099-01-01T00:00:00Z"}`},
		"/installation/repositories":          {status: http.StatusOK, body: `{"repositories":[{"html_url":"https://github.com/acme/widgets"}]}`},
	}}
	discoverer, err := NewDiscoverer(deterministicCredentials(t), boundary)
	if err != nil {
		t.Fatalf("create discoverer: %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, discoverErr := discoverer.Discover(t.Context())
		if discoverErr != nil || len(result.InstallationFailures) != 0 || len(result.Repositories) != 1 || result.Repositories[0] != (Repository{InstallationID: 11, URL: "https://github.com/acme/widgets"}) {
			t.Fatalf("discovery = (%#v, %v)", result, discoverErr)
		}
	}
	assertDiscoveryAuthentication(t, boundary.requests)
}

func assertDiscoveryAuthentication(t *testing.T, requests []*http.Request) {
	t.Helper()
	tokenRequests := 0
	for _, request := range requests {
		authorization := request.Header.Get("Authorization")
		if request.URL.Path == "/installation/repositories" {
			if authorization != "token scripted-installation-token" {
				t.Error("repository request did not use installation authentication")
			}
		} else if !strings.HasPrefix(authorization, "Bearer ") {
			t.Error("App request did not use JWT authentication")
		}
		if request.URL.Path == "/app/installations/11/access_tokens" {
			tokenRequests++
		}
	}
	if tokenRequests != 1 {
		t.Fatalf("installation token requests = %d, want one cached token", tokenRequests)
	}
}

func TestAuthenticatedDiscoveryAPIUsesSDK(t *testing.T) {
	t.Parallel()

	api, _ := authenticatedDiscoveryFixture(t)

	installations, _, err := api.ListInstallations(context.Background(), 1)
	if err != nil || len(installations) != 1 || installations[0] != 11 {
		t.Fatalf("installations = (%#v, %v)", installations, err)
	}
	repositories, _, err := api.ListRepositories(context.Background(), 11, 1)
	if err != nil || len(repositories) != 1 || repositories[0] != "https://github.com/acme/widgets" {
		t.Fatalf("repositories = (%#v, %v)", repositories, err)
	}
}

func TestAuthenticatedDiscoveryAPICachesAndCreatesInstallationClients(t *testing.T) {
	t.Parallel()

	api, installationClient := authenticatedDiscoveryFixture(t)
	cached, err := api.installationClient(11)
	if err != nil || cached != installationClient {
		t.Fatalf("cached client = (%p, %v)", cached, err)
	}
	created, err := api.installationClient(12)
	if err != nil || created == nil {
		t.Fatalf("created client = (%p, %v)", created, err)
	}
}

func authenticatedDiscoveryFixture(t *testing.T) (*authenticatedDiscoveryAPI, *githubapi.Client) {
	t.Helper()
	discoverer, err := NewDiscoverer(deterministicCredentials(t), nil)
	if err != nil {
		t.Fatalf("create discoverer: %v", err)
	}
	api, ok := discoverer.API.(*authenticatedDiscoveryAPI)
	if !ok {
		t.Fatalf("discovery API type = %T", discoverer.API)
	}
	transport := &protocolTransport{responses: map[string]protocolResponse{
		"/app/installations":         {status: http.StatusOK, body: `[{"id":11}]`},
		"/installation/repositories": {status: http.StatusOK, body: `{"repositories":[{"html_url":"https://github.com/acme/widgets"}]}`},
	}}
	api.appClient = newTestGitHubClient(t, transport)
	installationClient := newTestGitHubClient(t, transport)
	api.installationClients[11] = installationClient
	return api, installationClient
}

func TestAuthenticatedDiscoveryAPIReportsAuthenticationAndRequestFailures(t *testing.T) {
	t.Parallel()

	if _, err := NewDiscoverer(Credentials{AppID: 1, PrivateKey: "invalid"}, http.DefaultTransport); err == nil {
		t.Fatal("NewDiscoverer accepted invalid credentials")
	}
	expectedErr := errors.New("unavailable")
	failingClient := newTestGitHubClient(t, &protocolTransport{err: expectedErr})
	api := &authenticatedDiscoveryAPI{
		credentials:         Credentials{AppID: 1, PrivateKey: "invalid"},
		baseTransport:       &protocolTransport{err: expectedErr},
		appClient:           failingClient,
		installationClients: make(map[InstallationID]*githubapi.Client),
	}
	if _, _, err := api.ListInstallations(context.Background(), 1); err == nil {
		t.Fatal("ListInstallations succeeded")
	}
	api.installationClients[11] = api.appClient
	if _, _, err := api.ListRepositories(context.Background(), 11, 1); err == nil {
		t.Fatal("ListRepositories succeeded")
	}
	delete(api.installationClients, 11)
	if _, err := api.installationClient(11); err == nil {
		t.Fatal("installationClient accepted invalid credentials")
	}
}

func TestDiscoveryReportsGitHubClientConstructionFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("client construction failed")
	if _, err := newGitHubClient(nil); err == nil {
		t.Fatal("newGitHubClient accepted nil HTTP client")
	}
	failingFactory := func(*http.Client) (*githubapi.Client, error) { return nil, expectedErr }
	if _, err := buildDiscoverer(deterministicCredentials(t), http.DefaultTransport, failingFactory); !errors.Is(err, expectedErr) {
		t.Fatalf("buildDiscoverer error = %v", err)
	}
	api := &authenticatedDiscoveryAPI{
		credentials:         deterministicCredentials(t),
		baseTransport:       http.DefaultTransport,
		clientFactory:       failingFactory,
		installationClients: make(map[InstallationID]*githubapi.Client),
	}
	if _, _, err := api.ListRepositories(context.Background(), 11, 1); !errors.Is(err, expectedErr) {
		t.Fatalf("ListRepositories error = %v", err)
	}
}

func newTestGitHubClient(t *testing.T, transport http.RoundTripper) *githubapi.Client {
	t.Helper()
	client, err := githubapi.NewClient(githubapi.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("create test GitHub client: %v", err)
	}
	return client
}

func TestDiscovererReportsMissingAPIAndInstallationEnumerationFailure(t *testing.T) {
	t.Parallel()

	if _, err := (Discoverer{}).Discover(context.Background()); err == nil {
		t.Fatal("Discover succeeded without API")
	}
	discoverer := Discoverer{API: failingDiscoveryAPI{}}
	if _, err := discoverer.Discover(context.Background()); err == nil {
		t.Fatal("Discover succeeded when installation enumeration failed")
	}
}

type failingDiscoveryAPI struct{}

func (failingDiscoveryAPI) ListInstallations(context.Context, int) ([]InstallationID, int, error) {
	return nil, 0, errors.New("unavailable")
}

func (failingDiscoveryAPI) ListRepositories(context.Context, InstallationID, int) ([]string, int, error) {
	return nil, 0, errors.New("unexpected call")
}
