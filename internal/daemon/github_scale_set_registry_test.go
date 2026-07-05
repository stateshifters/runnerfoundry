package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestGitHubScaleSetRegistryListsScaleSets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v3/repos/meza/repo/actions/runners/registration-token":
			if request.Header.Get("Authorization") != "Bearer secret" {
				t.Fatalf("expected PAT auth, got %q", request.Header.Get("Authorization"))
			}
			writer.WriteHeader(http.StatusCreated)
			writeString(t, writer, `{"token":"registration"}`)
		case "/api/v3/actions/runner-registration":
			if request.Header.Get("Authorization") != "RemoteAuth registration" {
				t.Fatalf("expected remote auth, got %q", request.Header.Get("Authorization"))
			}
			writeJSON(t, writer, actionsServiceAdminConnection{URL: serverURL(request) + "/actions-service", Token: "admin"})
		case "/actions-service/_apis/runtime/runnerscalesets":
			if request.Header.Get("Authorization") != "Bearer admin" {
				t.Fatalf("expected admin auth, got %q", request.Header.Get("Authorization"))
			}
			if request.URL.Query().Get("runnerGroupId") != "9" {
				t.Fatalf("expected runner group query, got %q", request.URL.RawQuery)
			}
			writeString(t, writer, `{"count":2,"value":[{"id":1,"name":"linux"},{"id":2,"name":"stale"}]}`)
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
		}
	}))
	defer server.Close()

	registry := githubScaleSetRegistry{
		repositoryURL: server.URL + "/meza/repo",
		token:         "secret",
		httpClient:    server.Client(),
	}
	runnerScaleSets, err := registry.ListRunnerScaleSets(context.Background(), 9)
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if len(runnerScaleSets) != 2 || runnerScaleSets[1].Name != "stale" {
		t.Fatalf("expected scale sets, got %#v", runnerScaleSets)
	}
}

func TestGitHubScaleSetRegistrySurfacesHTTPFailures(t *testing.T) {
	tests := map[string]struct {
		handler  http.HandlerFunc
		expected string
	}{
		"registration token status": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				http.Error(writer, "failed", http.StatusUnauthorized)
			},
			expected: "get runner registration token",
		},
		"registration token missing token": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusCreated)
				writeString(t, writer, `{}`)
			},
			expected: "missing token",
		},
		"registration token bad json": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(http.StatusCreated)
				writeString(t, writer, `{`)
			},
			expected: "decode response",
		},
		"admin connection status": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				if strings.Contains(request.URL.Path, "registration-token") {
					writer.WriteHeader(http.StatusCreated)
					writeString(t, writer, `{"token":"registration"}`)
					return
				}
				http.Error(writer, "failed", http.StatusUnauthorized)
			},
			expected: "get actions service admin connection",
		},
		"admin connection missing values": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				if strings.Contains(request.URL.Path, "registration-token") {
					writer.WriteHeader(http.StatusCreated)
					writeString(t, writer, `{"token":"registration"}`)
					return
				}
				writeString(t, writer, `{}`)
			},
			expected: "missing url or token",
		},
		"admin connection bad json": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				if strings.Contains(request.URL.Path, "registration-token") {
					writer.WriteHeader(http.StatusCreated)
					writeString(t, writer, `{"token":"registration"}`)
					return
				}
				writeString(t, writer, `{`)
			},
			expected: "decode response",
		},
		"scale set list status": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				switch {
				case strings.Contains(request.URL.Path, "registration-token"):
					writer.WriteHeader(http.StatusCreated)
					writeString(t, writer, `{"token":"registration"}`)
				case request.URL.Path == "/api/v3/actions/runner-registration":
					writeJSON(t, writer, actionsServiceAdminConnection{URL: serverURL(request) + "/actions-service", Token: "admin"})
				default:
					http.Error(writer, "failed", http.StatusUnauthorized)
				}
			},
			expected: "list runner scale sets",
		},
		"scale set list bad json": {
			handler: func(writer http.ResponseWriter, request *http.Request) {
				switch {
				case strings.Contains(request.URL.Path, "registration-token"):
					writer.WriteHeader(http.StatusCreated)
					writeString(t, writer, `{"token":"registration"}`)
				case request.URL.Path == "/api/v3/actions/runner-registration":
					writeJSON(t, writer, actionsServiceAdminConnection{URL: serverURL(request) + "/actions-service", Token: "admin"})
				default:
					writeString(t, writer, `{`)
				}
			},
			expected: "decode response",
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			server := httptest.NewServer(testCase.handler)
			defer server.Close()
			registry := githubScaleSetRegistry{
				repositoryURL: server.URL + "/meza/repo",
				token:         "secret",
				httpClient:    server.Client(),
			}
			_, err := registry.ListRunnerScaleSets(context.Background(), 1)
			if err == nil || !strings.Contains(err.Error(), testCase.expected) {
				t.Fatalf("expected %q error, got %v", testCase.expected, err)
			}
		})
	}
}

func TestGitHubScaleSetRegistryHelperFunctions(t *testing.T) {
	owner, repository, err := repositoryOwnerAndName("https://github.com/meza/github-scaler/")
	if err != nil {
		t.Fatalf("expected repository identity success, got %v", err)
	}
	if owner != "meza" || repository != "github-scaler" {
		t.Fatalf("expected owner and repository, got %q %q", owner, repository)
	}
	_, _, err = repositoryOwnerAndName("https://github.com/meza")
	if err == nil {
		t.Fatal("expected invalid repository url")
	}
	_, _, err = repositoryOwnerAndName("http://[::1/meza/repo")
	if err == nil {
		t.Fatal("expected repository url parse error")
	}
	apiEndpoint, err := apiURL("https://github.com/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err != nil {
		t.Fatalf("expected api url success, got %v", err)
	}
	if !strings.HasPrefix(apiEndpoint, "https://api.github.com/") {
		t.Fatalf("expected hosted api endpoint, got %q", apiEndpoint)
	}
	enterpriseEndpoint, err := apiURL("https://ghe.example.com/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err != nil {
		t.Fatalf("expected enterprise api url success, got %v", err)
	}
	if !strings.Contains(enterpriseEndpoint, "/api/v3/repos/") {
		t.Fatalf("expected enterprise api path, got %q", enterpriseEndpoint)
	}
	_, err = apiURL("http://[::1/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err == nil {
		t.Fatal("expected api url parse error")
	}
	_, err = runnerRegistrationURL("http://[::1/meza/github-scaler")
	if err == nil {
		t.Fatal("expected runner registration url parse error")
	}
}

func TestRunnerRegistrationURLUsesGitHubAPIHost(t *testing.T) {
	registrationEndpoint, err := runnerRegistrationURL("https://ghe.example.com/meza/github-scaler")
	if err != nil {
		t.Fatalf("expected runner registration url success, got %v", err)
	}
	if registrationEndpoint != "https://ghe.example.com/api/v3/actions/runner-registration" {
		t.Fatalf("expected runner registration endpoint, got %q", registrationEndpoint)
	}

	hostedRegistrationEndpoint, err := runnerRegistrationURL("https://github.com/meza/github-scaler")
	if err != nil {
		t.Fatalf("expected hosted runner registration url success, got %v", err)
	}
	if hostedRegistrationEndpoint != "https://api.github.com/actions/runner-registration" {
		t.Fatalf("expected hosted runner registration endpoint, got %q", hostedRegistrationEndpoint)
	}
}

func TestGitHubScaleSetRegistryConstructsDefaultHTTPClient(t *testing.T) {
	registry := newGitHubScaleSetRegistry("https://github.com/meza/github-scaler", "secret")
	if registry.repositoryURL != "https://github.com/meza/github-scaler" || registry.token != "secret" || registry.httpClient == nil {
		t.Fatalf("expected registry defaults, got %#v", registry)
	}
}

func TestGitHubScaleSetRegistrySurfacesRequestConstructionFailures(t *testing.T) {
	registry := githubScaleSetRegistry{repositoryURL: "http://[::1/meza/repo", token: "secret", httpClient: http.DefaultClient}
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

	registry := githubScaleSetRegistry{repositoryURL: "https://github.com/meza/repo", token: "secret", httpClient: http.DefaultClient}
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
