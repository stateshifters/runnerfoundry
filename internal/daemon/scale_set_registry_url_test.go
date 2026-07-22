package daemon

import "testing"

func TestRepositoryOwnerAndNameReturnsRepositoryIdentity(t *testing.T) {
	owner, repository, err := repositoryOwnerAndName("https://github.com/meza/github-scaler/")
	if err != nil {
		t.Fatalf("repository identity: %v", err)
	}
	if owner != "meza" || repository != "github-scaler" {
		t.Fatalf("repository identity = %q %q", owner, repository)
	}
}

func TestAPIURLUsesGitHubHostedAPI(t *testing.T) {
	apiEndpoint, err := apiURL("https://github.com/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err != nil {
		t.Fatalf("hosted API URL: %v", err)
	}
	if apiEndpoint != "https://api.github.com/repos/meza/github-scaler/actions/runners/registration-token" {
		t.Fatalf("hosted API endpoint = %q", apiEndpoint)
	}
}

func TestAPIURLUsesGitHubEnterpriseAPI(t *testing.T) {
	apiEndpoint, err := apiURL("https://ghe.example.com/meza/github-scaler", "/repos/meza/github-scaler/actions/runners/registration-token")
	if err != nil {
		t.Fatalf("enterprise API URL: %v", err)
	}
	if apiEndpoint != "https://ghe.example.com/api/v3/repos/meza/github-scaler/actions/runners/registration-token" {
		t.Fatalf("enterprise API endpoint = %q", apiEndpoint)
	}
}

func TestRunnerRegistrationURLUsesGitHubHostedAPI(t *testing.T) {
	registrationEndpoint, err := runnerRegistrationURL("https://github.com/meza/github-scaler")
	if err != nil {
		t.Fatalf("hosted runner registration URL: %v", err)
	}
	if registrationEndpoint != "https://api.github.com/actions/runner-registration" {
		t.Fatalf("hosted runner registration endpoint = %q", registrationEndpoint)
	}
}

func TestRunnerRegistrationURLUsesGitHubEnterpriseAPI(t *testing.T) {
	registrationEndpoint, err := runnerRegistrationURL("https://ghe.example.com/meza/github-scaler")
	if err != nil {
		t.Fatalf("enterprise runner registration URL: %v", err)
	}
	if registrationEndpoint != "https://ghe.example.com/api/v3/actions/runner-registration" {
		t.Fatalf("enterprise runner registration endpoint = %q", registrationEndpoint)
	}
}
