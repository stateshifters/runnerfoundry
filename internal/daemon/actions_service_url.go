package daemon

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

type actionsServiceURL struct {
	parsed *url.URL
}

func parseActionsServiceURL(rawURL string, repository repositoryTarget) (actionsServiceURL, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return actionsServiceURL{}, fmt.Errorf("parse actions service url: %w", err)
	}
	if err := requireHTTPSURL(parsedURL, "actions service url"); err != nil {
		return actionsServiceURL{}, err
	}
	if parsedURL.Fragment != "" {
		return actionsServiceURL{}, errors.New("actions service url must not include a fragment")
	}
	if isGitHubHostedRepository(repository.URL) {
		if err := validateGitHubHostedActionsServiceURL(parsedURL); err != nil {
			return actionsServiceURL{}, err
		}
		return actionsServiceURL{parsed: parsedURL}, nil
	}
	if !sameHTTPSOrigin(parsedURL, repository.URL) {
		return actionsServiceURL{}, errors.New("actions service url must use the repository origin")
	}
	return actionsServiceURL{parsed: parsedURL}, nil
}

func validateGitHubHostedActionsServiceURL(parsedURL *url.URL) error {
	if net.ParseIP(parsedURL.Hostname()) != nil {
		return errors.New("actions service url must not use an IP address for GitHub-hosted repositories")
	}
	hostname := strings.TrimSuffix(strings.ToLower(parsedURL.Hostname()), ".")
	if !strings.HasSuffix(hostname, ".actions.githubusercontent.com") {
		return errors.New("actions service url must use a trusted GitHub Actions host")
	}
	if effectiveHTTPSPort(parsedURL) != "443" {
		return errors.New("actions service url must use the default HTTPS port")
	}
	return nil
}

func (serviceURL actionsServiceURL) String() string {
	return serviceURL.parsed.String()
}

func isGitHubHostedRepository(repositoryURL *url.URL) bool {
	hostname := strings.TrimSuffix(strings.ToLower(repositoryURL.Hostname()), ".")
	return hostname == "github.com" || hostname == "www.github.com"
}

func effectiveHTTPSPort(parsedURL *url.URL) string {
	if parsedURL.Port() == "" {
		return "443"
	}
	return parsedURL.Port()
}

func sameHTTPSOrigin(candidate *url.URL, origin *url.URL) bool {
	return candidate.Scheme == "https" && origin.Scheme == "https" &&
		strings.EqualFold(strings.TrimSuffix(candidate.Hostname(), "."), strings.TrimSuffix(origin.Hostname(), ".")) &&
		effectiveHTTPSPort(candidate) == effectiveHTTPSPort(origin)
}

func sameOriginRedirectPolicy(origin *url.URL) func(*http.Request, []*http.Request) error {
	return func(request *http.Request, _ []*http.Request) error {
		if request.URL.User != nil || !sameHTTPSOrigin(request.URL, origin) {
			return fmt.Errorf("refuse redirect from %s to untrusted origin %s", origin.Redacted(), request.URL.Redacted())
		}
		return nil
	}
}
