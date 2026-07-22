package githubapp

import (
	"context"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/config"
)

// InstallationID identifies a GitHub App installation.
type InstallationID int64

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

type Discoverer struct {
	API DiscoveryAPI
}

func (discoverer Discoverer) Discover(ctx context.Context) (DiscoveryResult, error) {
	installationIDs, err := discoverer.installations(ctx)
	if err != nil {
		return DiscoveryResult{}, err
	}
	result := DiscoveryResult{InstallationFailures: map[InstallationID]error{}}
	for _, installationID := range installationIDs {
		repositories, repositoryErr := discoverer.repositories(ctx, installationID)
		if repositoryErr != nil {
			result.InstallationFailures[installationID] = repositoryErr
			continue
		}
		result.Repositories = append(result.Repositories, repositories...)
	}
	return result, nil
}

func (discoverer Discoverer) installations(ctx context.Context) ([]InstallationID, error) {
	var installationIDs []InstallationID
	for page := 1; page != 0; {
		pageInstallations, nextPage, err := discoverer.API.ListInstallations(ctx, page)
		if err != nil {
			return nil, fmt.Errorf("list GitHub App installations: %w", err)
		}
		installationIDs = append(installationIDs, pageInstallations...)
		page = nextPage
	}
	return installationIDs, nil
}

func (discoverer Discoverer) repositories(ctx context.Context, installationID InstallationID) ([]Repository, error) {
	var repositories []Repository
	for page := 1; page != 0; {
		pageRepositories, nextPage, err := discoverer.API.ListRepositories(ctx, installationID, page)
		if err != nil {
			return nil, fmt.Errorf("list repositories for installation %d: %w", installationID, err)
		}
		for _, repositoryURL := range pageRepositories {
			repositories = append(repositories, Repository{InstallationID: installationID, URL: repositoryURL})
		}
		page = nextPage
	}
	return repositories, nil
}

func NewDiscoverer(credentials config.GitHubAppCredentials) (Discoverer, error) {
	api, err := newGitHubAPI(credentials)
	if err != nil {
		return Discoverer{}, err
	}
	return Discoverer{API: api}, nil
}
