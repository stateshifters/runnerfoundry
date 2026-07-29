package daemon

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestGitHubScaleSetRegistryPreservesProtocolAuthorization(t *testing.T) {
	credentials := testAppCredentials(t)
	transport := &orderedRegistryTransport{expected: registryAuthorizationExpectations}

	registry, err := newGitHubScaleSetRegistryWithTransport(
		"https://github.com/meza/RunnerFoundry", githubapp.InstallationID(7), credentials, transport,
	)
	if err != nil {
		t.Fatal(err)
	}

	scaleSets, err := registry.ListRunnerScaleSets(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(scaleSets) != 1 || scaleSets[0].ID != 42 || scaleSets[0].Name != "linux" {
		t.Fatalf("scale sets = %#v", scaleSets)
	}
}

var registryAuthorizationExpectations = []expectedRegistryRequest{
	{
		path: "/app/installations/7/access_tokens", authorizationMatches: bearerAuthorization,
		statusCode: http.StatusCreated,
		body:       `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`,
	},
	{
		path:                 "/repos/meza/RunnerFoundry/actions/runners/registration-token",
		authorizationMatches: exactAuthorization("token installation-token"),
		statusCode:           http.StatusCreated,
		body:                 `{"token":"registration-token"}`,
	},
	{
		path: "/actions/runner-registration", authorizationMatches: exactAuthorization("RemoteAuth registration-token"),
		statusCode: http.StatusOK,
		body:       `{"url":"https://pipelines.actions.githubusercontent.com/actions-service","token":"admin-token"}`,
	},
	{
		path: "/actions-service/_apis/runtime/runnerscalesets", authorizationMatches: exactAuthorization("Bearer admin-token"),
		statusCode: http.StatusOK,
		body:       `{"count":1,"value":[{"id":42,"name":"linux","runnerGroupId":1}]}`,
	},
}

type expectedRegistryRequest struct {
	path                 string
	authorizationMatches func(string) bool
	statusCode           int
	body                 string
}

type orderedRegistryTransport struct {
	expected []expectedRegistryRequest
	next     int
}

func (transport *orderedRegistryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.next >= len(transport.expected) {
		return nil, fmt.Errorf("unexpected request %s %s", request.Method, request.URL)
	}
	expected := transport.expected[transport.next]
	transport.next++
	if request.URL.Path != expected.path {
		return nil, fmt.Errorf("request path = %q, want %q", request.URL.Path, expected.path)
	}
	authorization := request.Header.Get("Authorization")
	if !expected.authorizationMatches(authorization) {
		return nil, fmt.Errorf("authorization for %s = %q", expected.path, authorization)
	}
	return responseForRequest(request, expected.statusCode, expected.body), nil
}

func bearerAuthorization(authorization string) bool {
	return strings.HasPrefix(authorization, "Bearer ")
}

func exactAuthorization(expected string) func(string) bool {
	return func(actual string) bool { return actual == expected }
}

func responseForRequest(request *http.Request, statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}
