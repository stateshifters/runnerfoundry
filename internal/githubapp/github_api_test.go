package githubapp

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-github/v80/github"
	"github.com/meza/RunnerFoundry/internal/config"
)

func TestGitHubAPIDiscoversInstallationsAndRepositoriesThroughSDK(t *testing.T) {
	api, _ := githubSDKFixture(t)
	installations, nextPage, err := api.ListInstallations(context.Background(), 1)
	if err != nil || len(installations) != 1 || installations[0] != 11 || nextPage != 2 {
		t.Fatalf("unexpected installations %#v, page %d, error %v", installations, nextPage, err)
	}

	repositories, _, err := api.ListRepositories(context.Background(), 11, 1)
	if err != nil || len(repositories) != 1 || repositories[0] != "https://github.com/acme/repo" {
		t.Fatalf("unexpected repositories %#v, error %v", repositories, err)
	}
}

func TestGitHubAPIReusesCachedInstallationClient(t *testing.T) {
	api, installationClient := githubSDKFixture(t)
	if cached, err := api.installationClient(11); err != nil || cached != installationClient {
		t.Fatalf("expected cached installation client, got %v, %v", cached, err)
	}
}

func githubSDKFixture(t *testing.T) (*githubAPI, *github.Client) {
	t.Helper()
	api, err := newGitHubAPI(validCredentials(t))
	if err != nil {
		t.Fatalf("expected API client, got %v", err)
	}
	transport := staticGitHubTransport{responses: map[string]staticGitHubResponse{
		"/app/installations": {
			body:    `[{"id":11}]`,
			headers: http.Header{"Link": {`<https://api.github.test/app/installations?page=2>; rel="next"`}},
		},
		"/installation/repositories": {body: `{"total_count":1,"repositories":[{"html_url":"https://github.com/acme/repo"}]}`},
	}}
	api.appClient = newTestGitHubClient(t, transport)
	installationClient := newTestGitHubClient(t, transport)
	api.installationClients[11] = installationClient
	return api, installationClient
}

func TestGitHubAPIRejectsAnInvalidPrivateKey(t *testing.T) {
	if _, err := NewDiscoverer(config.GitHubAppCredentials{AppID: 1, PrivateKey: "invalid"}); err == nil {
		t.Fatal("expected invalid private key error")
	}
}

func TestGitHubAPIReportsRequestFailures(t *testing.T) {
	api, err := newGitHubAPI(validCredentials(t))
	if err != nil {
		t.Fatal(err)
	}
	api.appClient = newTestGitHubClient(t, staticGitHubTransport{err: errors.New("unavailable")})
	if _, _, err = api.ListInstallations(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "request installations") {
		t.Fatalf("expected installation request error, got %v", err)
	}
	api.installationClients[9] = api.appClient
	if _, _, err = api.ListRepositories(context.Background(), 9, 1); err == nil || !strings.Contains(err.Error(), "request installation repositories") {
		t.Fatalf("expected repository request error, got %v", err)
	}
}

func TestGitHubAPICreatesAndCachesInstallationClients(t *testing.T) {
	api, err := newGitHubAPI(validCredentials(t))
	if err != nil {
		t.Fatal(err)
	}
	client, err := api.installationClient(10)
	if err != nil {
		t.Fatalf("expected installation transport, got %v", err)
	}
	cached, err := api.installationClient(10)
	if err != nil || cached != client {
		t.Fatalf("expected cached installation client, got %v, %v", cached, err)
	}
	if _, err := NewDiscoverer(validCredentials(t)); err != nil {
		t.Fatalf("expected discoverer construction, got %v", err)
	}
}

func TestGitHubAPIReportsInstallationClientAuthenticationFailure(t *testing.T) {
	api := &githubAPI{
		appID: 1, privateKey: []byte("invalid"), baseTransport: staticGitHubTransport{},
		installationClients: map[InstallationID]*github.Client{},
	}
	if _, _, err := api.ListRepositories(context.Background(), 12, 1); err == nil {
		t.Fatal("expected installation client construction failure")
	}
}

func validCredentials(t *testing.T) config.GitHubAppCredentials {
	t.Helper()
	primeOne, primeOneValid := new(big.Int).SetString("C49B48187124EE531F26C48AEBB4E3FD4C069D73DD3DA35D0AA87F46834A9BF3F2D3CD90BEDCB8199DBD2552CE670106A95ED0A3E0AE559FA68E846339D542CB", 16)
	primeTwo, primeTwoValid := new(big.Int).SetString("F24E7544C243EC2DC2D3CD84F1B7CB1561FD7A25FE50D139AA338957EC2C5C64D68D8927518766C38CF2A0EA5B17F7D4C784B75B33F0316A0D82248F24533DF7", 16)
	if !primeOneValid || !primeTwoValid {
		t.Fatal("invalid deterministic RSA fixture")
	}
	modulus := new(big.Int).Mul(primeOne, primeTwo)
	one := big.NewInt(1)
	totient := new(big.Int).Mul(new(big.Int).Sub(primeOne, one), new(big.Int).Sub(primeTwo, one))
	privateExponent := new(big.Int).ModInverse(big.NewInt(65537), totient)
	privateKey := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{N: modulus, E: 65537},
		D:         privateExponent,
		Primes:    []*big.Int{primeOne, primeTwo},
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return config.GitHubAppCredentials{AppID: 1, PrivateKey: string(encoded)}
}

func newTestGitHubClient(t *testing.T, transport http.RoundTripper) *github.Client {
	t.Helper()
	client := github.NewClient(&http.Client{Transport: transport})
	baseURL, err := url.Parse("https://api.github.test/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL
	return client
}

type staticGitHubResponse struct {
	status  int
	body    string
	headers http.Header
}

type staticGitHubTransport struct {
	responses map[string]staticGitHubResponse
	err       error
}

func (transport staticGitHubTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.err != nil {
		return nil, transport.err
	}
	configured := transport.responses[request.URL.Path]
	status := configured.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     configured.headers,
		Body:       io.NopCloser(strings.NewReader(configured.body)),
		Request:    request,
	}, nil
}
