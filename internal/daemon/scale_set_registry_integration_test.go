//go:build integration

package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestGitHubScaleSetRegistryListsScaleSets(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.serveAdminConnection()
	fixture.mux.HandleFunc("/actions-service/_apis/runtime/runnerscalesets", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer admin" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.URL.Query().Get("runnerGroupId") != "9" {
			t.Errorf("runnerGroupId = %q", request.URL.Query().Get("runnerGroupId"))
		}
		writeString(t, writer, `{"count":2,"value":[{"id":1,"name":"linux"},{"id":2,"name":"stale"}]}`)
	})

	runnerScaleSets, err := fixture.registry.ListRunnerScaleSets(context.Background(), 9)
	if err != nil {
		t.Fatalf("list runner scale sets: %v", err)
	}
	if len(runnerScaleSets) != 2 || runnerScaleSets[1].Name != "stale" {
		t.Fatalf("runner scale sets = %#v", runnerScaleSets)
	}
}

func TestGitHubScaleSetRegistryReportsRegistrationTokenStatus(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.mux.HandleFunc("/api/v3/repos/meza/repo/actions/runners/registration-token", func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "failed", http.StatusUnauthorized)
	})
	assertRegistryErrorContains(t, fixture.registry, "get runner registration token")
}

func TestGitHubScaleSetRegistryRejectsMissingRegistrationToken(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.mux.HandleFunc("/api/v3/repos/meza/repo/actions/runners/registration-token", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		writeString(t, writer, `{}`)
	})
	assertRegistryErrorContains(t, fixture.registry, "missing token")
}

func TestGitHubScaleSetRegistryRejectsMalformedRegistrationTokenResponse(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.mux.HandleFunc("/api/v3/repos/meza/repo/actions/runners/registration-token", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		writeString(t, writer, `{`)
	})
	assertRegistryErrorContains(t, fixture.registry, "decode response")
}

func TestGitHubScaleSetRegistryReportsAdminConnectionStatus(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.mux.HandleFunc("/api/v3/actions/runner-registration", func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "failed", http.StatusUnauthorized)
	})
	assertRegistryErrorContains(t, fixture.registry, "get actions service admin connection")
}

func TestGitHubScaleSetRegistryRejectsMissingAdminConnectionValues(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.mux.HandleFunc("/api/v3/actions/runner-registration", func(writer http.ResponseWriter, _ *http.Request) {
		writeString(t, writer, `{}`)
	})
	assertRegistryErrorContains(t, fixture.registry, "missing url or token")
}

func TestGitHubScaleSetRegistryRejectsMalformedAdminConnectionResponse(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.mux.HandleFunc("/api/v3/actions/runner-registration", func(writer http.ResponseWriter, _ *http.Request) {
		writeString(t, writer, `{`)
	})
	assertRegistryErrorContains(t, fixture.registry, "decode response")
}

func TestGitHubScaleSetRegistryReportsScaleSetListStatus(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.serveAdminConnection()
	fixture.mux.HandleFunc("/actions-service/_apis/runtime/runnerscalesets", func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "failed", http.StatusUnauthorized)
	})
	assertRegistryErrorContains(t, fixture.registry, "list runner scale sets")
}

func TestGitHubScaleSetRegistryRejectsMalformedScaleSetListResponse(t *testing.T) {
	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.serveAdminConnection()
	fixture.mux.HandleFunc("/actions-service/_apis/runtime/runnerscalesets", func(writer http.ResponseWriter, _ *http.Request) {
		writeString(t, writer, `{`)
	})
	assertRegistryErrorContains(t, fixture.registry, "decode response")
}

func TestGitHubScaleSetRegistryRejectsCrossOriginRedirect(t *testing.T) {
	var redirectedRequests atomic.Int32
	destination := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		writeString(t, writer, `{"count":0,"value":[]}`)
	}))
	t.Cleanup(destination.Close)

	fixture := newRegistryIntegrationFixture(t)
	fixture.serveRegistrationToken()
	fixture.serveAdminConnection()
	fixture.mux.HandleFunc("/actions-service/_apis/runtime/runnerscalesets", func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, destination.URL, http.StatusFound)
	})

	assertRegistryErrorContains(t, fixture.registry, "untrusted origin")
	if redirectedRequests.Load() != 0 {
		t.Fatalf("cross-origin redirect sent %d requests", redirectedRequests.Load())
	}
}

type registryIntegrationFixture struct {
	t        *testing.T
	mux      *http.ServeMux
	server   *httptest.Server
	registry githubScaleSetRegistry
}

func newRegistryIntegrationFixture(t *testing.T) *registryIntegrationFixture {
	t.Helper()
	mux := http.NewServeMux()
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	return &registryIntegrationFixture{
		t:      t,
		mux:    mux,
		server: server,
		registry: githubScaleSetRegistry{
			repositoryURL:      server.URL + "/meza/repo",
			installationClient: server.Client(),
			protocolClient:     server.Client(),
		},
	}
}

func (fixture *registryIntegrationFixture) serveRegistrationToken() {
	fixture.mux.HandleFunc("/api/v3/repos/meza/repo/actions/runners/registration-token", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		writeString(fixture.t, writer, `{"token":"registration"}`)
	})
}

func (fixture *registryIntegrationFixture) serveAdminConnection() {
	fixture.mux.HandleFunc("/api/v3/actions/runner-registration", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "RemoteAuth registration" {
			http.Error(writer, "missing remote auth", http.StatusUnauthorized)
			return
		}
		writeJSON(fixture.t, writer, actionsServiceAdminConnectionResponse{URL: fixture.server.URL + "/actions-service", Token: "admin"})
	})
}

func assertRegistryErrorContains(t *testing.T, registry githubScaleSetRegistry, expected string) {
	t.Helper()
	_, err := registry.ListRunnerScaleSets(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("registry error = %v, want text %q", err, expected)
	}
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
		t.Fatalf("write JSON response: %v", err)
	}
}
