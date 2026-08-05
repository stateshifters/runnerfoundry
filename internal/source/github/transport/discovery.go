package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/bradleyfalzon/ghinstallation/v2"
	githubapi "github.com/google/go-github/v88/github"
)

const discoveryPageSize = 100

type InstallationID int64

type Credentials struct {
	AppID      int64
	PrivateKey string
}

type Repository struct {
	InstallationID InstallationID
	URL            string
}

type DiscoveryResult struct {
	Repositories         []Repository
	InstallationFailures map[InstallationID]error
}

type DiscoveryAPI interface {
	ListInstallations(ctx context.Context, page int) ([]InstallationID, int, error)
	ListRepositories(ctx context.Context, installationID InstallationID, page int) ([]string, int, error)
}

// Discoverer enumerates repositories available to a GitHub App. A failure for
// one installation is returned in DiscoveryResult without hiding repositories
// from other installations; failure to enumerate installations aborts the operation.
type Discoverer struct {
	API DiscoveryAPI
}

type githubClientFactory func(*http.Client) (*githubapi.Client, error)

func (discoverer Discoverer) Discover(ctx context.Context) (DiscoveryResult, error) {
	if discoverer.API == nil {
		return DiscoveryResult{}, errors.New("GitHub discovery API is required")
	}
	installationIDs, err := discoverer.listAllInstallations(ctx)
	if err != nil {
		return DiscoveryResult{}, err
	}
	result := DiscoveryResult{InstallationFailures: make(map[InstallationID]error)}
	for _, installationID := range installationIDs {
		repositories, repositoryErr := discoverer.listAllRepositories(ctx, installationID)
		if repositoryErr != nil {
			result.InstallationFailures[installationID] = repositoryErr
			continue
		}
		result.Repositories = append(result.Repositories, repositories...)
	}
	return result, nil
}

func (discoverer Discoverer) listAllInstallations(ctx context.Context) ([]InstallationID, error) {
	var installationIDs []InstallationID
	for page := 1; page != 0; {
		pageInstallationIDs, nextPage, err := discoverer.API.ListInstallations(ctx, page)
		if err != nil {
			return nil, fmt.Errorf("list GitHub App installations: %w", err)
		}
		installationIDs = append(installationIDs, pageInstallationIDs...)
		page = nextPage
	}
	return installationIDs, nil
}

func (discoverer Discoverer) listAllRepositories(ctx context.Context, installationID InstallationID) ([]Repository, error) {
	var repositories []Repository
	for page := 1; page != 0; {
		repositoryURLs, nextPage, err := discoverer.API.ListRepositories(ctx, installationID, page)
		if err != nil {
			return nil, fmt.Errorf("list repositories for installation %d: %w", installationID, err)
		}
		for _, repositoryURL := range repositoryURLs {
			repositories = append(repositories, Repository{InstallationID: installationID, URL: repositoryURL})
		}
		page = nextPage
	}
	return repositories, nil
}

// NewDiscoverer authenticates as a GitHub App using the supplied base
// transport. The transport is used for every App and installation request.
func NewDiscoverer(credentials Credentials, baseTransport http.RoundTripper) (Discoverer, error) {
	return buildDiscoverer(credentials, baseTransport, newGitHubClient)
}

func buildDiscoverer(
	credentials Credentials,
	baseTransport http.RoundTripper,
	clientFactory githubClientFactory,
) (Discoverer, error) {
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	appTransport, err := ghinstallation.NewAppsTransport(
		baseTransport,
		credentials.AppID,
		[]byte(credentials.PrivateKey),
	)
	if err != nil {
		return Discoverer{}, fmt.Errorf("create GitHub App transport: %w", err)
	}
	appClient, err := clientFactory(&http.Client{Transport: appTransport})
	if err != nil {
		return Discoverer{}, fmt.Errorf("create GitHub App client: %w", err)
	}
	return Discoverer{API: &authenticatedDiscoveryAPI{
		credentials:         credentials,
		baseTransport:       baseTransport,
		appClient:           appClient,
		clientFactory:       clientFactory,
		installationClients: make(map[InstallationID]*githubapi.Client),
	}}, nil
}

func newGitHubClient(client *http.Client) (*githubapi.Client, error) {
	githubClient, err := githubapi.NewClient(githubapi.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("create GitHub SDK client: %w", err)
	}
	return githubClient, nil
}

type authenticatedDiscoveryAPI struct {
	credentials         Credentials
	baseTransport       http.RoundTripper
	appClient           *githubapi.Client
	clientFactory       githubClientFactory
	installationClients map[InstallationID]*githubapi.Client
	mutex               sync.Mutex
}

func (api *authenticatedDiscoveryAPI) ListInstallations(
	ctx context.Context,
	page int,
) ([]InstallationID, int, error) {
	installations, response, err := api.appClient.Apps.ListInstallations(ctx, &githubapi.ListOptions{
		Page: page, PerPage: discoveryPageSize,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("request installations: %w", err)
	}
	installationIDs := make([]InstallationID, 0, len(installations))
	for _, installation := range installations {
		installationIDs = append(installationIDs, InstallationID(installation.GetID()))
	}
	return installationIDs, response.NextPage, nil
}

func (api *authenticatedDiscoveryAPI) ListRepositories(
	ctx context.Context,
	installationID InstallationID,
	page int,
) ([]string, int, error) {
	client, err := api.installationClient(installationID)
	if err != nil {
		return nil, 0, err
	}
	repositoryList, response, err := client.Apps.ListRepos(ctx, &githubapi.ListOptions{
		Page: page, PerPage: discoveryPageSize,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("request installation repositories: %w", err)
	}
	repositoryURLs := make([]string, 0, len(repositoryList.Repositories))
	for _, repository := range repositoryList.Repositories {
		repositoryURLs = append(repositoryURLs, repository.GetHTMLURL())
	}
	return repositoryURLs, response.NextPage, nil
}

func (api *authenticatedDiscoveryAPI) installationClient(installationID InstallationID) (*githubapi.Client, error) {
	api.mutex.Lock()
	defer api.mutex.Unlock()
	if client := api.installationClients[installationID]; client != nil {
		return client, nil
	}
	transport, err := ghinstallation.New(
		api.baseTransport,
		api.credentials.AppID,
		int64(installationID),
		[]byte(api.credentials.PrivateKey),
	)
	if err != nil {
		return nil, fmt.Errorf("create installation transport: %w", err)
	}
	client, err := api.clientFactory(&http.Client{Transport: transport})
	if err != nil {
		return nil, fmt.Errorf("create installation client: %w", err)
	}
	api.installationClients[installationID] = client
	return client, nil
}
