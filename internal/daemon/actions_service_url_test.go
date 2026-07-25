package daemon

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestActionsServiceURLAcceptsGitHubHostedActionsOrigin(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://github.com/meza/RunnerFoundry")
	serviceURL, err := parseActionsServiceURL("https://pipelines.actions.githubusercontent.com/tenant", repository)
	if err != nil {
		t.Fatalf("parse hosted Actions service URL: %v", err)
	}
	if serviceURL.String() != "https://pipelines.actions.githubusercontent.com/tenant" {
		t.Fatalf("Actions service URL = %q", serviceURL.String())
	}
}

func TestActionsServiceURLRejectsUnrelatedHostedOrigin(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://github.com/meza/RunnerFoundry")
	_, err := parseActionsServiceURL("https://attacker.example/actions", repository)
	if err == nil || !strings.Contains(err.Error(), "trusted GitHub Actions host") {
		t.Fatalf("unrelated origin error = %v", err)
	}
}

func TestActionsServiceURLRejectsHostedIPTarget(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://github.com/meza/RunnerFoundry")
	_, err := parseActionsServiceURL("https://127.0.0.1/actions", repository)
	if err == nil || !strings.Contains(err.Error(), "IP address") {
		t.Fatalf("IP target error = %v", err)
	}
}

func TestActionsServiceURLRejectsHostedUserInformation(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://github.com/meza/RunnerFoundry")
	_, err := parseActionsServiceURL("https://user@pipelines.actions.githubusercontent.com/actions", repository)
	if err == nil || !strings.Contains(err.Error(), "user information") {
		t.Fatalf("userinfo error = %v", err)
	}
}

func TestActionsServiceURLRejectsHostedUnexpectedPort(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://github.com/meza/RunnerFoundry")
	_, err := parseActionsServiceURL("https://pipelines.actions.githubusercontent.com:8443/actions", repository)
	if err == nil || !strings.Contains(err.Error(), "port") {
		t.Fatalf("unexpected port error = %v", err)
	}
}

func TestActionsServiceURLAcceptsEnterpriseRepositoryOrigin(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/runnerfoundry")
	serviceURL, err := parseActionsServiceURL("https://ghe.example.com/actions-service", repository)
	if err != nil {
		t.Fatalf("parse enterprise Actions service URL: %v", err)
	}
	if serviceURL.String() != "https://ghe.example.com/actions-service" {
		t.Fatalf("Actions service URL = %q", serviceURL.String())
	}
}

func TestActionsServiceURLRejectsDifferentEnterpriseOrigin(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/runnerfoundry")
	_, err := parseActionsServiceURL("https://actions.example.com/actions-service", repository)
	if err == nil || !strings.Contains(err.Error(), "repository origin") {
		t.Fatalf("different enterprise origin error = %v", err)
	}
}

func TestActionsServiceURLRejectsDifferentEnterprisePort(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com:8443/meza/runnerfoundry")
	_, err := parseActionsServiceURL("https://ghe.example.com/actions-service", repository)
	if err == nil || !strings.Contains(err.Error(), "repository origin") {
		t.Fatalf("different enterprise port error = %v", err)
	}
}

func TestActionsServiceURLRejectsHTTPSDowngrade(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/runnerfoundry")
	_, err := parseActionsServiceURL("http://ghe.example.com/actions-service", repository)
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("HTTPS downgrade error = %v", err)
	}
}

func TestActionsServiceURLReportsMalformedAuthority(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/runnerfoundry")
	if _, err := parseActionsServiceURL("https://[::1/actions", repository); err == nil {
		t.Fatal("expected malformed Actions service URL failure")
	}
}

func TestActionsServiceURLRejectsFragment(t *testing.T) {
	repository := repositoryTargetForTest(t, "https://ghe.example.com/meza/runnerfoundry")
	if _, err := parseActionsServiceURL("https://ghe.example.com/actions#fragment", repository); err == nil {
		t.Fatal("expected Actions service URL fragment rejection")
	}
}

func TestSameOriginRedirectPolicyAcceptsHTTPSPathRedirect(t *testing.T) {
	origin, err := url.Parse("https://ghe.example.com/actions")
	if err != nil {
		t.Fatal(err)
	}
	redirect, err := http.NewRequest(http.MethodGet, "https://ghe.example.com/replacement", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if err := sameOriginRedirectPolicy(origin)(redirect, nil); err != nil {
		t.Fatalf("same-origin redirect: %v", err)
	}
}

func TestSameOriginRedirectPolicyRejectsCrossOriginRedirect(t *testing.T) {
	origin, err := url.Parse("https://ghe.example.com/actions")
	if err != nil {
		t.Fatal(err)
	}
	redirect, err := http.NewRequest(http.MethodGet, "https://attacker.example/replacement", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if err := sameOriginRedirectPolicy(origin)(redirect, nil); err == nil {
		t.Fatal("expected cross-origin redirect rejection")
	}
}

func TestSameOriginRedirectPolicyRejectsHTTPSDowngrade(t *testing.T) {
	origin, err := url.Parse("https://ghe.example.com/actions")
	if err != nil {
		t.Fatal(err)
	}
	redirect, err := http.NewRequest(http.MethodGet, "http://ghe.example.com/replacement", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if err := sameOriginRedirectPolicy(origin)(redirect, nil); err == nil {
		t.Fatal("expected HTTPS downgrade rejection")
	}
}

func repositoryTargetForTest(t *testing.T, repositoryURL string) repositoryTarget {
	t.Helper()
	target, err := parseRepositoryTarget(repositoryURL)
	if err != nil {
		t.Fatalf("parse repository target: %v", err)
	}
	return target
}
