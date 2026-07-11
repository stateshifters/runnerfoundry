package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func TestNewGitHubClientBuildsOfficialClientAndRegistry(t *testing.T) {
	client, err := newGitHubClient("https://github.com/meza/agent-playground", 1, testAppCredentials(t))
	if err != nil {
		t.Fatalf("expected client success, got %v", err)
	}
	if client.Client == nil {
		t.Fatal("expected official scale set client")
	}
	if client.Registry == nil {
		t.Fatal("expected scale set registry")
	}
	if client.CreateMessageSessionClient == nil {
		t.Fatal("expected message session factory")
	}
}

func TestNewGitHubClientRejectsInvalidRepositoryURL(t *testing.T) {
	_, err := newGitHubClient("://github.com/meza/agent-playground", 1, config.GitHubAppCredentials{AppID: 1, PrivateKey: "bad"})
	if err == nil || !strings.Contains(err.Error(), "create scaleset client") {
		t.Fatalf("expected client creation error, got %v", err)
	}
}

func TestNewGitHubClientRejectsInvalidAppPrivateKey(t *testing.T) {
	_, err := newGitHubClient("https://github.com/meza/agent-playground", 1, config.GitHubAppCredentials{AppID: 1, PrivateKey: "bad"})
	if err == nil || !strings.Contains(err.Error(), "create scaleset registry") {
		t.Fatalf("expected registry authentication error, got %v", err)
	}
}

func TestGitHubScaleSetClientListsScaleSetsThroughRegistry(t *testing.T) {
	registry := &fakeScaleSetRegistry{
		runnerScaleSets: []upstream.RunnerScaleSet{{ID: 1, Name: "linux"}},
	}
	client := githubScaleSetClient{Registry: registry}

	runnerScaleSets, err := client.ListRunnerScaleSets(context.Background(), 7)
	if err != nil {
		t.Fatalf("expected list success, got %v", err)
	}
	if registry.runnerGroupID != 7 {
		t.Fatalf("expected runner group ID 7, got %d", registry.runnerGroupID)
	}
	if len(runnerScaleSets) != 1 || runnerScaleSets[0].Name != "linux" {
		t.Fatalf("expected listed scale sets, got %#v", runnerScaleSets)
	}
}

func TestGitHubScaleSetClientWrapsRegistryFailures(t *testing.T) {
	client := githubScaleSetClient{Registry: &fakeScaleSetRegistry{err: errors.New("registry failed")}}

	_, err := client.ListRunnerScaleSets(context.Background(), 7)
	if err == nil || !strings.Contains(err.Error(), "list runner scale sets") {
		t.Fatalf("expected list error, got %v", err)
	}
}

func TestGitHubScaleSetClientCreatesMessageSessionAdapter(t *testing.T) {
	created := false
	client := githubScaleSetClient{
		CreateMessageSessionClient: func(
			ctx context.Context,
			runnerScaleSetID int,
			owner string,
			options ...upstream.HTTPOption,
		) (*upstream.MessageSessionClient, error) {
			created = true
			if runnerScaleSetID != 9 {
				t.Fatalf("expected scale set ID 9, got %d", runnerScaleSetID)
			}
			if owner != "host" {
				t.Fatalf("expected owner host, got %q", owner)
			}
			return &upstream.MessageSessionClient{}, nil
		},
	}

	messageClient, err := client.MessageSessionClient(context.Background(), 9, "host")
	if err != nil {
		t.Fatalf("expected message session success, got %v", err)
	}
	if !created {
		t.Fatal("expected message session factory call")
	}
	if messageClient.Client == nil {
		t.Fatal("expected wrapped message session client")
	}
}

func TestGitHubScaleSetClientWrapsMessageSessionFailures(t *testing.T) {
	client := githubScaleSetClient{
		CreateMessageSessionClient: func(
			ctx context.Context,
			runnerScaleSetID int,
			owner string,
			options ...upstream.HTTPOption,
		) (*upstream.MessageSessionClient, error) {
			return nil, errors.New("session failed")
		},
	}

	_, err := client.MessageSessionClient(context.Background(), 9, "host")
	if err == nil || !strings.Contains(err.Error(), "create message session client") {
		t.Fatalf("expected message session error, got %v", err)
	}
}

type fakeScaleSetRegistry struct {
	runnerGroupID   int
	runnerScaleSets []upstream.RunnerScaleSet
	err             error
}

func (registry *fakeScaleSetRegistry) ListRunnerScaleSets(
	ctx context.Context,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	registry.runnerGroupID = runnerGroupID
	if registry.err != nil {
		return nil, registry.err
	}
	return registry.runnerScaleSets, nil
}
