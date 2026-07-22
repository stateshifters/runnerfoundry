package githubapp

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v80/github"
	"github.com/meza/RunnerFoundry/internal/config"
)

const discoveryPageSize = 100

type githubAPI struct {
	appID               int64
	privateKey          []byte
	baseTransport       http.RoundTripper
	appClient           *github.Client
	installationClients map[InstallationID]*github.Client
	clientMutex         sync.Mutex
}

func newGitHubAPI(credentials config.GitHubAppCredentials) (*githubAPI, error) {
	baseTransport := http.DefaultTransport
	appTransport, err := ghinstallation.NewAppsTransport(baseTransport, credentials.AppID, []byte(credentials.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("create GitHub App transport: %w", err)
	}
	return &githubAPI{
		appID:               credentials.AppID,
		privateKey:          []byte(credentials.PrivateKey),
		baseTransport:       baseTransport,
		appClient:           github.NewClient(&http.Client{Transport: appTransport}),
		installationClients: map[InstallationID]*github.Client{},
	}, nil
}

func (api *githubAPI) ListInstallations(ctx context.Context, page int) ([]InstallationID, int, error) {
	installations, response, err := api.appClient.Apps.ListInstallations(ctx, &github.ListOptions{Page: page, PerPage: discoveryPageSize})
	if err != nil {
		return nil, 0, fmt.Errorf("request installations: %w", err)
	}
	installationIDs := make([]InstallationID, 0, len(installations))
	for _, installation := range installations {
		installationIDs = append(installationIDs, InstallationID(installation.GetID()))
	}
	return installationIDs, response.NextPage, nil
}

func (api *githubAPI) ListRepositories(ctx context.Context, installationID InstallationID, page int) ([]string, int, error) {
	client, err := api.installationClient(installationID)
	if err != nil {
		return nil, 0, err
	}
	repositoryList, response, err := client.Apps.ListRepos(ctx, &github.ListOptions{Page: page, PerPage: discoveryPageSize})
	if err != nil {
		return nil, 0, fmt.Errorf("request installation repositories: %w", err)
	}
	repositoryURLs := make([]string, 0, len(repositoryList.Repositories))
	for _, repository := range repositoryList.Repositories {
		repositoryURLs = append(repositoryURLs, repository.GetHTMLURL())
	}
	return repositoryURLs, response.NextPage, nil
}

func (api *githubAPI) installationClient(installationID InstallationID) (*github.Client, error) {
	api.clientMutex.Lock()
	defer api.clientMutex.Unlock()
	if client := api.installationClients[installationID]; client != nil {
		return client, nil
	}
	transport, err := ghinstallation.New(api.baseTransport, api.appID, int64(installationID), api.privateKey)
	if err != nil {
		return nil, fmt.Errorf("create installation transport: %w", err)
	}
	client := github.NewClient(&http.Client{Transport: transport})
	api.installationClients[installationID] = client
	return client, nil
}
