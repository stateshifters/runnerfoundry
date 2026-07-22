package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/meza/github-scaler/internal/config"
)

func TestRepositoryOwnerAndNameRejectsInvalidPath(t *testing.T) {
	if _, _, err := repositoryOwnerAndName("https://github.com/meza"); err == nil {
		t.Fatal("expected invalid repository url")
	}
}

func TestRepositoryOwnerAndNameRejectsNonHTTPSURL(t *testing.T) {
	if _, _, err := repositoryOwnerAndName("http://github.com/meza/repo"); err == nil {
		t.Fatal("expected non-https repository url error")
	}
}

func TestRepositoryOwnerAndNameReportsMalformedURL(t *testing.T) {
	if _, _, err := repositoryOwnerAndName("http://[::1/meza/repo"); err == nil {
		t.Fatal("expected repository url parse error")
	}
}

func TestAPIURLReportsMalformedRepositoryURL(t *testing.T) {
	if _, err := apiURL("http://[::1/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token"); err == nil {
		t.Fatal("expected api url parse error")
	}
}

func TestRunnerRegistrationURLReportsMalformedRepositoryURL(t *testing.T) {
	if _, err := runnerRegistrationURL("http://[::1/meza/github-scaler"); err == nil {
		t.Fatal("expected runner registration url parse error")
	}
}

func TestRunnerRegistrationURLRejectsNonHTTPSURL(t *testing.T) {
	if _, err := runnerRegistrationURL("http://github.com/meza/github-scaler"); err == nil {
		t.Fatal("expected non-https runner registration url error")
	}
}

func TestGitHubScaleSetRegistryConstructsDefaultHTTPClient(t *testing.T) {
	registry, err := newGitHubScaleSetRegistry("https://github.com/meza/github-scaler", 1, testAppCredentials(t))
	if err != nil {
		t.Fatalf("expected registry, got %v", err)
	}
	if registry.repositoryURL != "https://github.com/meza/github-scaler" || registry.httpClient == nil {
		t.Fatalf("expected registry defaults, got %#v", registry)
	}
	if registry.httpClient == http.DefaultClient {
		t.Fatal("expected registry to avoid the unbounded default HTTP client")
	}
	if registry.httpClient.Timeout == 0 {
		t.Fatal("expected registry HTTP client timeout")
	}
}

func TestGitHubScaleSetRegistryRejectsInvalidPrivateKey(t *testing.T) {
	_, err := newGitHubScaleSetRegistry("https://github.com/meza/github-scaler", 1, config.GitHubAppCredentials{AppID: 1, PrivateKey: "bad"})
	if err == nil {
		t.Fatal("expected invalid private key error")
	}
}

func TestGitHubScaleSetRegistrySurfacesRequestConstructionFailures(t *testing.T) {
	registry := githubScaleSetRegistry{repositoryURL: "http://[::1/meza/repo", httpClient: http.DefaultClient}
	if _, err := registry.runnerRegistrationToken(context.Background()); err == nil {
		t.Fatal("expected runner registration token url error")
	}
	if _, err := registry.actionsServiceAdminConnection(context.Background(), "registration"); err == nil {
		t.Fatal("expected admin connection url error")
	}
	if err := requireHTTPSURL(&url.URL{Scheme: "https"}, "test url"); err == nil {
		t.Fatal("expected missing host error")
	}
}

func TestGitHubScaleSetRegistrySurfacesTransportFailure(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	serviceURL, err := parseActionsServiceURL("https://ghe.example.com/actions", repository)
	if err != nil {
		t.Fatal(err)
	}
	registry := githubScaleSetRegistry{httpClient: &http.Client{Transport: failingRoundTripper{}}}
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: serviceURL, Token: "admin"}, 1); err == nil {
		t.Fatal("expected transport error")
	}
}

func TestGitHubScaleSetRegistrySurfacesResponseCloseFailure(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	serviceURL, err := parseActionsServiceURL("https://ghe.example.com/actions", repository)
	if err != nil {
		t.Fatal(err)
	}
	registry := githubScaleSetRegistry{httpClient: &http.Client{Transport: staticResponseTransport{
		response: &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       closeFailingBody{Reader: bytes.NewReader([]byte(`{"count":0,"value":[]}`))},
		},
	}}}
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: serviceURL, Token: "admin"}, 1); err == nil {
		t.Fatal("expected close error")
	}
}

func TestRunnerRegistrationTokenSurfacesTransportFailure(t *testing.T) {
	registry := githubScaleSetRegistry{httpClient: &http.Client{Transport: failingRoundTripper{}}}
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	if _, err := registry.requestRunnerRegistrationToken(context.Background(), target); err == nil {
		t.Fatal("expected registration transport failure")
	}
}

func TestRunnerRegistrationTokenSurfacesResponseReadFailure(t *testing.T) {
	registry := registryWithStaticResponse(http.StatusCreated, readFailingBody{})
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	if _, err := registry.requestRunnerRegistrationToken(context.Background(), target); err == nil {
		t.Fatal("expected registration response read failure")
	}
}

func TestRunnerRegistrationTokenSurfacesResponseCloseFailure(t *testing.T) {
	registry := registryWithStaticResponse(http.StatusCreated, closeFailingBody{Reader: bytes.NewReader([]byte(`{}`))})
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	if _, err := registry.requestRunnerRegistrationToken(context.Background(), target); err == nil {
		t.Fatal("expected registration response close failure")
	}
}

func TestActionsServiceAdminConnectionSurfacesTransportFailure(t *testing.T) {
	registry := githubScaleSetRegistry{
		repositoryURL: "https://ghe.example.com/meza/repo",
		httpClient:    &http.Client{Transport: failingRoundTripper{}},
	}
	target := repositoryTargetForTest(t, registry.repositoryURL)
	if _, err := registry.requestActionsServiceAdminConnection(context.Background(), "registration", target); err == nil {
		t.Fatal("expected admin connection transport failure")
	}
}

func TestActionsServiceAdminConnectionSurfacesResponseReadFailure(t *testing.T) {
	response := &http.Response{StatusCode: http.StatusOK, Status: "200 OK"}
	if _, err := validateActionsServiceAdminResponse(response, nil, errors.New("read failed"), nil); err == nil {
		t.Fatal("expected admin connection response read failure")
	}
}

func TestActionsServiceAdminConnectionSurfacesResponseCloseFailure(t *testing.T) {
	response := &http.Response{StatusCode: http.StatusOK, Status: "200 OK"}
	if _, err := validateActionsServiceAdminResponse(response, nil, nil, errors.New("close failed")); err == nil {
		t.Fatal("expected admin connection response close failure")
	}
}

func TestActionsServiceAdminConnectionRejectsUntrustedURL(t *testing.T) {
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	response := actionsServiceAdminConnectionResponse{URL: "http://ghe.example.com/actions", Token: "admin"}
	if _, err := validatedActionsServiceAdminConnection(response, target); err == nil {
		t.Fatal("expected untrusted admin connection URL failure")
	}
}

func TestRunnerScaleSetListSurfacesResponseReadFailure(t *testing.T) {
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	serviceURL, err := parseActionsServiceURL("https://ghe.example.com/actions", target)
	if err != nil {
		t.Fatal(err)
	}
	registry := registryWithStaticResponse(http.StatusOK, readFailingBody{})
	if _, err := registry.fetchRunnerScaleSets(
		context.Background(), actionsServiceAdminConnection{URL: serviceURL, Token: "admin"}, 1,
	); err == nil {
		t.Fatal("expected scale-set response read failure")
	}
}

func registryWithStaticResponse(status int, body io.ReadCloser) githubScaleSetRegistry {
	return githubScaleSetRegistry{httpClient: &http.Client{Transport: staticResponseTransport{response: &http.Response{
		StatusCode: status, Status: http.StatusText(status), Body: body,
	}}}}
}

type readFailingBody struct{}

func (readFailingBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (readFailingBody) Close() error             { return nil }

type failingRoundTripper struct{}

func (transport failingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, errors.New("transport failed")
}

type staticResponseTransport struct {
	response *http.Response
}

func (transport staticResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport.response, nil
}

type closeFailingBody struct {
	*bytes.Reader
}

func (body closeFailingBody) Close() error {
	return errors.New("close failed")
}
