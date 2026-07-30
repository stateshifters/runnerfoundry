package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
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
	if _, err := apiURL("http://[::1/meza/runnerfoundry", "/repos/meza/runnerfoundry/actions/runners/registration-token"); err == nil {
		t.Fatal("expected api url parse error")
	}
}

func TestRunnerRegistrationURLReportsMalformedRepositoryURL(t *testing.T) {
	if _, err := runnerRegistrationURL("http://[::1/meza/runnerfoundry"); err == nil {
		t.Fatal("expected runner registration url parse error")
	}
}

func TestRunnerRegistrationURLRejectsNonHTTPSURL(t *testing.T) {
	if _, err := runnerRegistrationURL("http://github.com/meza/RunnerFoundry"); err == nil {
		t.Fatal("expected non-https runner registration url error")
	}
}

func TestGitHubScaleSetRegistryRejectsInvalidPrivateKey(t *testing.T) {
	_, err := newGitHubScaleSetRegistry("https://github.com/meza/RunnerFoundry", 1, config.GitHubAppCredentials{AppID: 1, PrivateKey: "bad"})
	if err == nil {
		t.Fatal("expected invalid private key error")
	}
}

func TestGitHubScaleSetRegistrySurfacesRequestConstructionFailures(t *testing.T) {
	registry := githubScaleSetRegistry{
		repositoryURL: "http://[::1/meza/repo", installationClient: http.DefaultClient, protocolClient: http.DefaultClient,
	}
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
	registry := githubScaleSetRegistry{protocolClient: &http.Client{Transport: failingRoundTripper{}}}
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
	registry := githubScaleSetRegistry{protocolClient: &http.Client{Transport: staticResponseTransport{
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

func TestGitHubScaleSetRegistryStatusErrorsDoNotExposeCredentials(t *testing.T) {
	const credentials = "RemoteAuth registration-secret Bearer admin-secret"
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	serviceURL, err := parseActionsServiceURL("https://ghe.example.com/actions", repository)
	if err != nil {
		t.Fatal(err)
	}
	registry := registryWithStaticResponse(
		http.StatusUnauthorized, io.NopCloser(strings.NewReader(credentials)),
	)
	_, err = registry.fetchRunnerScaleSets(
		context.Background(), actionsServiceAdminConnection{URL: serviceURL, Token: "admin-secret"}, 1,
	)
	if err == nil {
		t.Fatal("expected status error")
	}
	if strings.Contains(err.Error(), "registration-secret") || strings.Contains(err.Error(), "admin-secret") {
		t.Fatalf("status error exposed credentials: %v", err)
	}
}

func TestRunnerRegistrationTokenSurfacesTransportFailure(t *testing.T) {
	registry := githubScaleSetRegistry{installationClient: &http.Client{Transport: failingRoundTripper{}}}
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
		repositoryURL:  "https://ghe.example.com/meza/repo",
		protocolClient: &http.Client{Transport: failingRoundTripper{}},
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

func TestRegistryWorkflowPropagatesEachInMemoryBoundaryFailure(t *testing.T) {
	malformed := githubScaleSetRegistry{repositoryURL: "invalid", installationClient: http.DefaultClient}
	if _, err := malformed.ListRunnerScaleSets(context.Background(), 1); err == nil {
		t.Fatal("expected registration failure")
	}
	transportFailure := githubScaleSetRegistry{
		repositoryURL:      "https://ghe.example.com/meza/repo",
		installationClient: &http.Client{Transport: failingRoundTripper{}},
	}
	if _, err := transportFailure.runnerRegistrationToken(context.Background()); err == nil {
		t.Fatal("expected registration request failure")
	}
	registrationClient := registryWithStaticResponse(http.StatusCreated,
		io.NopCloser(strings.NewReader(`{"token":"registration"}`))).installationClient
	registry := githubScaleSetRegistry{
		repositoryURL: "https://ghe.example.com/meza/repo", installationClient: registrationClient,
		protocolClient: &http.Client{Transport: failingRoundTripper{}},
	}
	if _, err := registry.ListRunnerScaleSets(context.Background(), 1); err == nil {
		t.Fatal("expected admin connection failure")
	}
}

func TestRegistryRejectsMalformedAndIncompleteInMemoryResponses(t *testing.T) {
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	for _, body := range []string{"invalid", `{}`} {
		registry := registryWithStaticResponse(http.StatusCreated, io.NopCloser(strings.NewReader(body)))
		registry.repositoryURL = target.URL.String()
		if _, err := registry.runnerRegistrationToken(context.Background()); err == nil {
			t.Fatalf("registration body %q was accepted", body)
		}
	}
	registry := registryWithStaticResponse(http.StatusUnauthorized, io.NopCloser(strings.NewReader(`{}`)))
	if _, err := registry.requestRunnerRegistrationToken(context.Background(), target); err == nil {
		t.Fatal("expected registration status failure")
	}
	registry = githubScaleSetRegistry{repositoryURL: target.URL.String(), protocolClient: &http.Client{Transport: failingRoundTripper{}}}
	if _, err := registry.actionsServiceAdminConnection(context.Background(), "registration"); err == nil {
		t.Fatal("expected admin request failure")
	}
}

func TestRegistryRejectsInvalidAdminAndScaleSetPayloadsInMemory(t *testing.T) {
	target := repositoryTargetForTest(t, "https://ghe.example.com/meza/repo")
	registry := registryWithStaticResponse(http.StatusOK, io.NopCloser(strings.NewReader("invalid")))
	if _, err := registry.requestActionsServiceAdminConnection(context.Background(), "registration", target); err == nil {
		t.Fatal("expected admin payload failure")
	}
	response := &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized"}
	if _, err := validateActionsServiceAdminResponse(response, nil, nil, nil); err == nil {
		t.Fatal("expected admin status failure")
	}
	if _, err := validatedActionsServiceAdminConnection(actionsServiceAdminConnectionResponse{}, target); err == nil {
		t.Fatal("expected incomplete admin connection failure")
	}
	serviceURL, err := parseActionsServiceURL("https://ghe.example.com/actions", target)
	if err != nil {
		t.Fatal(err)
	}
	registry = registryWithStaticResponse(http.StatusOK, io.NopCloser(strings.NewReader("invalid")))
	if _, err = registry.fetchRunnerScaleSets(context.Background(), actionsServiceAdminConnection{
		URL: serviceURL, Token: "admin",
	}, 1); err == nil {
		t.Fatal("expected scale-set payload failure")
	}
}

func registryWithStaticResponse(status int, body io.ReadCloser) githubScaleSetRegistry {
	client := &http.Client{Transport: staticResponseTransport{response: &http.Response{
		StatusCode: status, Status: http.StatusText(status), Body: body,
	}}}
	return githubScaleSetRegistry{installationClient: client, protocolClient: client}
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
