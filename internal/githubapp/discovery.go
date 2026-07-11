package githubapp

import (
	"context"
	"fmt"

	"github.com/meza/github-scaler/internal/config"
)

type Repository struct {
	InstallationID int64
	URL            string
}

type DiscoveryResult struct {
	Repositories         []Repository
	InstallationFailures map[int64]error
}

type DiscoveryAPI interface {
	ListInstallations(ctx context.Context, page int) ([]int64, int, error)
	ListRepositories(ctx context.Context, installationID int64, page int) ([]string, int, error)
}

type Discoverer struct {
	API DiscoveryAPI
}

func (discoverer Discoverer) Discover(ctx context.Context) (DiscoveryResult, error) {
	installationIDs, err := discoverer.installations(ctx)
	if err != nil {
		return DiscoveryResult{}, err
	}
	result := DiscoveryResult{InstallationFailures: map[int64]error{}}
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

func (discoverer Discoverer) installations(ctx context.Context) ([]int64, error) {
	var installationIDs []int64
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

func (discoverer Discoverer) repositories(ctx context.Context, installationID int64) ([]Repository, error) {
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
