package githubapp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-github/v80/github"
	"github.com/meza/github-scaler/internal/config"
)

func TestGitHubAPIDiscoversInstallationsAndRepositoriesThroughSDK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations":
			writer.Header().Set("Link", `<`+requestServerURL(request)+`/app/installations?page=2>; rel="next"`)
			writeResponse(t, writer, `[{"id":11}]`)
		case "/installation/repositories":
			writeResponse(t, writer, `{"total_count":1,"repositories":[{"html_url":"https://github.com/acme/repo"}]}`)
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
		}
	}))
	defer server.Close()

	api, err := newGitHubAPI(validCredentials(t))
	if err != nil {
		t.Fatalf("expected API client, got %v", err)
	}
	api.appClient = github.NewClient(server.Client())
	api.appClient.BaseURL = parseTestURL(t, api.appClient, server.URL)
	installationClient := github.NewClient(server.Client())
	installationClient.BaseURL = parseTestURL(t, installationClient, server.URL)
	api.installationClients[11] = installationClient

	installations, nextPage, err := api.ListInstallations(context.Background(), 1)
	if err != nil || len(installations) != 1 || installations[0] != 11 || nextPage != 2 {
		t.Fatalf("unexpected installations %#v, page %d, error %v", installations, nextPage, err)
	}
	repositories, _, err := api.ListRepositories(context.Background(), 11, 1)
	if err != nil || len(repositories) != 1 || repositories[0] != "https://github.com/acme/repo" {
		t.Fatalf("unexpected repositories %#v, error %v", repositories, err)
	}
	if cached, err := api.installationClient(11); err != nil || cached != installationClient {
		t.Fatalf("expected cached installation client, got %v, %v", cached, err)
	}
}

func TestGitHubAPISurfacesAuthenticationAndRequestFailures(t *testing.T) {
	if _, err := NewDiscoverer(config.GitHubAppCredentials{AppID: 1, PrivateKey: "invalid"}); err == nil {
		t.Fatal("expected invalid private key error")
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	api, err := newGitHubAPI(validCredentials(t))
	if err != nil {
		t.Fatal(err)
	}
	api.appClient = github.NewClient(server.Client())
	api.appClient.BaseURL = parseTestURL(t, api.appClient, server.URL)
	if _, _, err := api.ListInstallations(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "request installations") {
		t.Fatalf("expected installation request error, got %v", err)
	}
	installationClient := github.NewClient(server.Client())
	installationClient.BaseURL = parseTestURL(t, installationClient, server.URL)
	api.installationClients[9] = installationClient
	if _, _, err := api.ListRepositories(context.Background(), 9, 1); err == nil || !strings.Contains(err.Error(), "request installation repositories") {
		t.Fatalf("expected repository request error, got %v", err)
	}
	if _, err := api.installationClient(10); err != nil {
		t.Fatalf("expected installation transport, got %v", err)
	}
	if _, err := NewDiscoverer(validCredentials(t)); err != nil {
		t.Fatalf("expected discoverer construction, got %v", err)
	}
	invalidAPI := &githubAPI{
		appID: 1, privateKey: []byte("invalid"), baseTransport: http.DefaultTransport,
		installationClients: map[int64]*github.Client{},
	}
	if _, _, err := invalidAPI.ListRepositories(context.Background(), 12, 1); err == nil {
		t.Fatal("expected installation client construction failure")
	}
}

func validCredentials(t *testing.T) config.GitHubAppCredentials {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return config.GitHubAppCredentials{AppID: 1, PrivateKey: string(encoded)}
}

func requestServerURL(request *http.Request) string {
	return "http://" + request.Host
}

func parseTestURL(t *testing.T, client *github.Client, serverURL string) *url.URL {
	t.Helper()
	parsedURL, err := client.BaseURL.Parse(serverURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	return parsedURL
}

func writeResponse(t *testing.T, writer http.ResponseWriter, response string) {
	t.Helper()
	if _, err := writer.Write([]byte(response)); err != nil {
		t.Fatal(err)
	}
}
