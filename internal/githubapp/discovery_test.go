package githubapp

import (
	"context"
	"errors"
	"strings"
	"testing"
)

var _ InstallationID = Repository{}.InstallationID

func TestDiscoverReturnsRepositoriesAcrossInstallationsAndIsolatesFailures(t *testing.T) {
	discoverer := Discoverer{API: fakeDiscoveryAPI{
		installationPages: [][]InstallationID{{11}, {22, 33}},
		repositoryPages: map[InstallationID][][]string{
			11: {{"https://github.com/acme/one"}, {"https://github.com/acme/two"}},
			33: {{"https://github.com/other/three"}},
		},
		repositoryErrors: map[InstallationID]error{22: errors.New("forbidden")},
	}}
	result, err := discoverer.Discover(context.Background())
	if err != nil {
		t.Fatalf("expected discovery to continue, got %v", err)
	}
	if len(result.Repositories) != 3 {
		t.Fatalf("expected three repositories, got %#v", result.Repositories)
	}
	if result.Repositories[1].InstallationID != 11 || result.Repositories[1].URL != "https://github.com/acme/two" {
		t.Fatalf("unexpected paginated repository %#v", result.Repositories[1])
	}
	if !strings.Contains(result.InstallationFailures[22].Error(), "forbidden") {
		t.Fatalf("expected isolated installation failure, got %#v", result.InstallationFailures)
	}
}

func TestDiscoverRejectsInstallationDiscoveryFailure(t *testing.T) {
	discoverer := Discoverer{API: fakeDiscoveryAPI{installationError: errors.New("unavailable")}}
	if _, err := discoverer.Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("expected installation discovery error, got %v", err)
	}
}

type fakeDiscoveryAPI struct {
	installationPages [][]InstallationID
	installationError error
	repositoryPages   map[InstallationID][][]string
	repositoryErrors  map[InstallationID]error
}

func (api fakeDiscoveryAPI) ListInstallations(_ context.Context, page int) ([]InstallationID, int, error) {
	if api.installationError != nil {
		return nil, 0, api.installationError
	}
	if page > len(api.installationPages) {
		return nil, 0, nil
	}
	nextPage := 0
	if page < len(api.installationPages) {
		nextPage = page + 1
	}
	return api.installationPages[page-1], nextPage, nil
}

func (api fakeDiscoveryAPI) ListRepositories(_ context.Context, installationID InstallationID, page int) ([]string, int, error) {
	if err := api.repositoryErrors[installationID]; err != nil {
		return nil, 0, err
	}
	pages := api.repositoryPages[installationID]
	if page > len(pages) {
		return nil, 0, nil
	}
	nextPage := 0
	if page < len(pages) {
		nextPage = page + 1
	}
	return pages[page-1], nextPage, nil
}
