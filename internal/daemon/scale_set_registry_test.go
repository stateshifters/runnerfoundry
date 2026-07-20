package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRepositoryOwnerAndNameReturnsRepositoryIdentity(t *testing.T) {
	owner, repository, err := repositoryOwnerAndName("https://github.com/meza/github-scaler/")
	if err != nil {
		t.Fatalf("expected repository identity success, got %v", err)
	}
	if owner != "meza" || repository != "github-scaler" {
		t.Fatalf("expected owner and repository, got %q %q", owner, repository)
	}
}

func TestAPIURLUsesGitHubHostedAPI(t *testing.T) {
	apiEndpoint, err := apiURL("https://github.com/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err != nil {
		t.Fatalf("expected API URL success, got %v", err)
	}
	if apiEndpoint != "https://api.github.com/repos/meza/github-scaler/actions/runners/registration-token" {
		t.Fatalf("expected hosted API endpoint, got %q", apiEndpoint)
	}
}

func TestAPIURLUsesGitHubEnterpriseAPI(t *testing.T) {
	apiEndpoint, err := apiURL("https://ghe.example.com/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err != nil {
		t.Fatalf("expected enterprise API URL success, got %v", err)
	}
	if apiEndpoint != "https://ghe.example.com/api/v3/repos/meza/github-scaler/actions/runners/registration-token" {
		t.Fatalf("expected enterprise API endpoint, got %q", apiEndpoint)
	}
}

func TestRunnerRegistrationURLUsesGitHubHostedAPI(t *testing.T) {
	registrationEndpoint, err := runnerRegistrationURL("https://github.com/meza/github-scaler")
	if err != nil {
		t.Fatalf("expected hosted runner registration URL success, got %v", err)
	}
	if registrationEndpoint != "https://api.github.com/actions/runner-registration" {
		t.Fatalf("expected hosted runner registration endpoint, got %q", registrationEndpoint)
	}
}

func TestRunnerRegistrationURLUsesGitHubEnterpriseAPI(t *testing.T) {
	registrationEndpoint, err := runnerRegistrationURL("https://ghe.example.com/meza/github-scaler")
	if err != nil {
		t.Fatalf("expected enterprise runner registration URL success, got %v", err)
	}
	if registrationEndpoint != "https://ghe.example.com/api/v3/actions/runner-registration" {
		t.Fatalf("expected enterprise runner registration endpoint, got %q", registrationEndpoint)
	}
}

func TestGitHubScaleSetRegistryListsScaleSets(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v3/repos/meza/repo/actions/runners/registration-token":
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
			server := httptest.NewTLSServer(testCase.handler)
			defer server.Close()
			registry := githubScaleSetRegistry{
				repositoryURL: server.URL + "/meza/repo",
				httpClient:    server.Client(),
			}
			_, err := registry.ListRunnerScaleSets(context.Background(), 1)
			if err == nil || !strings.Contains(err.Error(), testCase.expected) {
				t.Fatalf("expected %q error, got %v", testCase.expected, err)
			}
		})
	}
}
