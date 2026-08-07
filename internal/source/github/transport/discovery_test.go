package transport_test

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestDiscovererReturnsRepositoriesAndPreservesInstallationFailures(t *testing.T) {
	t.Parallel()

	expectedFailure := errors.New("repository access denied")
	discoverer := transport.Discoverer{API: &discoveryAPIFake{
		installations: map[int]installationPage{
			1: {installationIDs: []transport.InstallationID{11}, nextPage: 2},
			2: {installationIDs: []transport.InstallationID{22}},
		},
		repositories: map[transport.InstallationID]map[int]repositoryPage{
			11: {
				1: {repositoryURLs: []string{"https://github.com/acme/one"}, nextPage: 2},
				2: {repositoryURLs: []string{"https://github.com/acme/two"}},
			},
		},
		repositoryErrors: map[transport.InstallationID]error{22: expectedFailure},
	}}

	result, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("discover repositories: %v", err)
	}
	if len(result.Repositories) != 2 {
		t.Fatalf("repositories = %#v, want two", result.Repositories)
	}
	if result.Repositories[1] != (transport.Repository{InstallationID: 11, URL: "https://github.com/acme/two"}) {
		t.Fatalf("second repository = %#v", result.Repositories[1])
	}
	if !errors.Is(result.InstallationFailures[22], expectedFailure) {
		t.Fatalf("installation failure = %v, want %v", result.InstallationFailures[22], expectedFailure)
	}
}

type installationPage struct {
	installationIDs []transport.InstallationID
	nextPage        int
}

type repositoryPage struct {
	repositoryURLs []string
	nextPage       int
}

type discoveryAPIFake struct {
	installations    map[int]installationPage
	repositories     map[transport.InstallationID]map[int]repositoryPage
	repositoryErrors map[transport.InstallationID]error
}

func (api *discoveryAPIFake) ListInstallations(
	_ context.Context,
	pageNumber int,
) ([]transport.InstallationID, int, error) {
	page := api.installations[pageNumber]
	return page.installationIDs, page.nextPage, nil
}

func (api *discoveryAPIFake) ListRepositories(
	_ context.Context,
	installationID transport.InstallationID,
	pageNumber int,
) ([]string, int, error) {
	if err := api.repositoryErrors[installationID]; err != nil {
		return nil, 0, err
	}
	page := api.repositories[installationID][pageNumber]
	return page.repositoryURLs, page.nextPage, nil
}
