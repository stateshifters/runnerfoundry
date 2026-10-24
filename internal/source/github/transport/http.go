package transport

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stateshifters/runnerfoundry/internal/buildinfo"
)

// TransportFactory transfers exclusive ownership of a fresh transport on each call.
type TransportFactory func() *http.Transport

func newHTTPClient(factory TransportFactory) (*retryablehttp.Client, error) {
	client := retryablehttp.NewClient()
	if factory != nil {
		transport := factory()
		if transport == nil {
			return nil, errors.New("GitHub transport factory returned nil")
		}
		if transport.IdleConnTimeout <= 0 {
			transport.IdleConnTimeout = 90 * time.Second
		}
		client.HTTPClient.Transport = transport
	}
	client.Logger = nil
	return client, nil
}

func newSDKClient(repository Repository, credentials Credentials, factory TransportFactory) (*upstream.Client, error) {
	httpClient, err := newHTTPClient(factory)
	if err != nil {
		return nil, err
	}
	client, err := upstream.NewClientWithGitHubApp(upstream.ClientWithGitHubAppConfig{
		GitHubConfigURL: repository.URL,
		GitHubAppAuth: upstream.GitHubAppAuth{
			ClientID:       strconv.FormatInt(credentials.AppID, 10),
			InstallationID: int64(repository.InstallationID),
			PrivateKey:     credentials.PrivateKey,
		},
		SystemInfo: upstream.SystemInfo{System: "runnerfoundry", Version: buildinfo.Version()},
	}, upstream.WithRetryableHTTPClint(httpClient))
	if err != nil {
		httpClient.HTTPClient.CloseIdleConnections()
		return nil, fmt.Errorf("create GitHub Actions scale-set client: %w", err)
	}
	return client, nil
}
