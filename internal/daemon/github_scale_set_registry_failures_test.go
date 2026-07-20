package daemon

import (
	"bytes"
	"context"
	"encoding/json"
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
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: "http://[::1", Token: "admin"}, 1); err == nil {
		t.Fatal("expected list url error")
	}
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: "%", Token: "admin"}, 1); err == nil {
		t.Fatal("expected list parse error")
	}
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: "http://example.com", Token: "admin"}, 1); err == nil {
		t.Fatal("expected non-https admin url error")
	}
	if err := requireHTTPSURL(&url.URL{Scheme: "https"}, "test url"); err == nil {
		t.Fatal("expected missing host error")
	}
}

func TestGitHubScaleSetRegistrySurfacesTransportFailure(t *testing.T) {
	registry := githubScaleSetRegistry{httpClient: &http.Client{Transport: failingRoundTripper{}}}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", http.NoBody)
	if err != nil {
		t.Fatalf("expected request success, got %v", err)
	}
	if err := registry.doJSON(request, http.StatusOK, &runnerScaleSetsResponse{}); err == nil {
		t.Fatal("expected transport error")
	}
}

func TestGitHubScaleSetRegistrySurfacesResponseCloseFailure(t *testing.T) {
	registry := githubScaleSetRegistry{httpClient: &http.Client{Transport: staticResponseTransport{
		response: &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       closeFailingBody{Reader: bytes.NewReader([]byte(`{"count":0,"value":[]}`))},
		},
	}}}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", http.NoBody)
	if err != nil {
		t.Fatalf("expected request success, got %v", err)
	}
	if err := registry.doJSON(request, http.StatusOK, &runnerScaleSetsResponse{}); err == nil {
		t.Fatal("expected close error")
	}
}

func TestGitHubScaleSetRegistrySurfacesConstructionHookFailures(t *testing.T) {
	originalNewHTTPRequest := newHTTPRequest
	originalParseURL := parseURL
	defer func() {
		newHTTPRequest = originalNewHTTPRequest
		parseURL = originalParseURL
	}()

	registry := githubScaleSetRegistry{repositoryURL: "https://github.com/meza/repo", httpClient: http.DefaultClient}
	newHTTPRequest = func(ctx context.Context, method string, requestURL string, body io.Reader) (*http.Request, error) {
		return nil, errors.New("request failed")
	}
	if _, err := registry.runnerRegistrationToken(context.Background()); err == nil {
		t.Fatal("expected runner registration token request error")
	}
	if _, err := registry.actionsServiceAdminConnection(context.Background(), "registration"); err == nil {
		t.Fatal("expected admin connection request error")
	}
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: "https://example.com", Token: "admin"}, 1); err == nil {
		t.Fatal("expected list request error")
	}

	newHTTPRequest = originalNewHTTPRequest
	parseURL = func(rawURL string) (*url.URL, error) {
		return nil, errors.New("parse failed")
	}
	if _, err := registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{URL: "https://example.com", Token: "admin"}, 1); err == nil {
		t.Fatal("expected list parse error")
	}
}

func serverURL(request *http.Request) string {
	if request.TLS != nil {
		return "https://" + request.Host
	}
	return "http://" + request.Host
}

func writeString(t *testing.T, writer http.ResponseWriter, response string) {
	t.Helper()
	if _, err := writer.Write([]byte(response)); err != nil {
		t.Fatalf("write response: %v", err)
	}
}

func writeJSON(t *testing.T, writer http.ResponseWriter, response any) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		t.Fatalf("write json response: %v", err)
	}
}

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
