package github

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestRepositoryClientLoadValueRejectsInvalidValue(t *testing.T) {
	t.Parallel()

	if _, err := repositoryClientLoadValue("not a client"); err == nil {
		t.Fatal("repositoryClientLoadValue() accepted an invalid cache value")
	}
}

func TestLoadRepositoryClientReusesCachedValue(t *testing.T) {
	t.Parallel()

	repository := transport.Repository{URL: "https://github.com/octo/repo", InstallationID: 17}
	want := ManagerClient{EnsureRunnerPool: func(context.Context, string, string) (transport.Registration, error) {
		return transport.Registration{}, nil
	}}
	manager := &Manager{
		clients: map[transport.Repository]ManagerClient{repository: want},
		factory: func(context.Context, transport.Repository) (ManagerClient, error) {
			return ManagerClient{}, errors.New("factory must not be called for a cached client")
		},
	}
	got, err := manager.loadRepositoryClient(context.Background(), repository)
	if err != nil {
		t.Fatalf("loadRepositoryClient() error = %v", err)
	}
	if got.EnsureRunnerPool == nil {
		t.Fatal("loadRepositoryClient() did not return the cached client")
	}
}
