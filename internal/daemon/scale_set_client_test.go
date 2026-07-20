package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func TestScaleSetMessageClientForwardsMessages(t *testing.T) {
	message := &upstream.RunnerScaleSetMessage{MessageID: 7}
	client := scaleSetMessageClient{Client: &fakeMessageClient{message: message}}
	receivedMessage, err := client.GetMessage(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("expected message success, got %v", err)
	}
	if receivedMessage.MessageID != 7 {
		t.Fatalf("expected forwarded message, got %#v", receivedMessage)
	}
	if err := client.DeleteMessage(context.Background(), 7); err != nil {
		t.Fatalf("expected delete success, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected close success, got %v", err)
	}
}

func TestScaleSetMessageClientWrapsErrors(t *testing.T) {
	client := scaleSetMessageClient{Client: &fakeMessageClient{
		getMessageErr: errors.New("get failed"),
		deleteErr:     errors.New("delete failed"),
		closeErr:      errors.New("close failed"),
	}}
	if _, err := client.GetMessage(context.Background(), 1, 2); err == nil {
		t.Fatal("expected get error")
	}
	if err := client.DeleteMessage(context.Background(), 7); err == nil {
		t.Fatal("expected delete error")
	}
	if err := client.Close(context.Background()); err == nil {
		t.Fatal("expected close error")
	}
}

func TestEnsureRunnerScaleSetReturnsExistingOrCreatesNew(t *testing.T) {
	existing, err := ensureRunnerScaleSet(context.Background(), &fakeScaleSetClient{
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
	}, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected existing success, got %v", err)
	}
	if existing.ID != 1 {
		t.Fatalf("expected existing scale set, got %#v", existing)
	}

	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	created, err := ensureRunnerScaleSet(context.Background(), client, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	if created.ID != 2 {
		t.Fatalf("expected created scale set, got %#v", created)
	}
}

func TestEnsureRunnerScaleSetUsesConfiguredLabels(t *testing.T) {
	client := &fakeScaleSetClient{createdScaleSet: &upstream.RunnerScaleSet{ID: 2}}
	_, err := ensureRunnerScaleSet(context.Background(), client, 10, config.ScaleSetConfig{Name: "linux"})
	if err != nil {
		t.Fatalf("expected create success, got %v", err)
	}
	assertScaleSetLabels(t, client.createdScaleSetRequest.Labels, []string{"linux"})
}

func TestRunnerHostnameReturnsHostName(t *testing.T) {
	if runnerHostname() == "" {
		t.Fatal("expected hostname")
	}
}

func TestRunnerHostnameFallsBackWhenHostNameFails(t *testing.T) {
	originalHostname := operatingSystemHostname
	defer func() {
		operatingSystemHostname = originalHostname
	}()
	operatingSystemHostname = func() (string, error) {
		return "", errors.New("hostname failed")
	}
	if runnerHostname() != "github-scaler" {
		t.Fatal("expected fallback hostname")
	}
}

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
		t.Fatalf("expected client URL validation error, got %v", err)
	}
}

func TestNewGitHubClientRequiresHTTPSRepository(t *testing.T) {
	_, err := newGitHubClient("http://github.com/meza/agent-playground", 1, config.GitHubAppCredentials{AppID: 1, PrivateKey: "bad"})
	if err == nil || !strings.Contains(err.Error(), "validate repository target") {
		t.Fatalf("expected secure repository validation error, got %v", err)
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
