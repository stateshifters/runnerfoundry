package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	upstream "github.com/actions/scaleset"
)

const scaleSetEndpoint = "_apis/runtime/runnerscalesets"

type scaleSetRegistry interface {
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
}

type githubScaleSetRegistry struct {
	repositoryURL string
	token         string
	httpClient    *http.Client
}

type githubRegistrationToken struct {
	Token string `json:"token"`
}

type actionsServiceAdminConnection struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type runnerScaleSetsResponse struct {
	Count           int                       `json:"count"`
	RunnerScaleSets []upstream.RunnerScaleSet `json:"value"`
}

var (
	newHTTPRequest = http.NewRequestWithContext
	parseURL       = url.Parse
)

func newGitHubScaleSetRegistry(repositoryURL string, token string) githubScaleSetRegistry {
	return githubScaleSetRegistry{
		repositoryURL: repositoryURL,
		token:         token,
		httpClient:    http.DefaultClient,
	}
}

func (registry githubScaleSetRegistry) ListRunnerScaleSets(
	ctx context.Context,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	registrationToken, err := registry.runnerRegistrationToken(ctx)
	if err != nil {
		return nil, err
	}
	adminConnection, err := registry.actionsServiceAdminConnection(ctx, registrationToken)
	if err != nil {
		return nil, err
	}
	return registry.fetchRunnerScaleSets(ctx, adminConnection, runnerGroupID)
}

func (registry githubScaleSetRegistry) runnerRegistrationToken(ctx context.Context) (string, error) {
	target, err := parseRepositoryTarget(registry.repositoryURL)
	if err != nil {
		return "", err
	}
	requestURL := apiURLFromParsed(target.URL, fmt.Sprintf("/repos/%s/%s/actions/runners/registration-token", target.Owner, target.Repository))
	request, err := newHTTPRequest(ctx, http.MethodPost, requestURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("create runner registration token request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+registry.token)

	var registrationToken githubRegistrationToken
	if err := registry.doJSON(request, http.StatusCreated, &registrationToken); err != nil {
		return "", fmt.Errorf("get runner registration token: %w", err)
	}
	if registrationToken.Token == "" {
		return "", errors.New("runner registration token response missing token")
	}
	return registrationToken.Token, nil
}

func (registry githubScaleSetRegistry) actionsServiceAdminConnection(
	ctx context.Context,
	registrationToken string,
) (actionsServiceAdminConnection, error) {
	requestURL, err := runnerRegistrationURL(registry.repositoryURL)
	if err != nil {
		return actionsServiceAdminConnection{}, err
	}
	bodyText := fmt.Sprintf(`{"url":%s,"runner_event":"register"}`, strconv.Quote(registry.repositoryURL))
	request, err := newHTTPRequest(ctx, http.MethodPost, requestURL, bytes.NewReader([]byte(bodyText)))
	if err != nil {
		return actionsServiceAdminConnection{}, fmt.Errorf("create actions service admin connection request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "RemoteAuth "+registrationToken)

	var adminConnection actionsServiceAdminConnection
	if err := registry.doJSON(request, http.StatusOK, &adminConnection); err != nil {
		return actionsServiceAdminConnection{}, fmt.Errorf("get actions service admin connection: %w", err)
	}
	if adminConnection.URL == "" || adminConnection.Token == "" {
		return actionsServiceAdminConnection{}, errors.New("actions service admin connection response missing url or token")
	}
	return adminConnection, nil
}

func (registry githubScaleSetRegistry) fetchRunnerScaleSets(
	ctx context.Context,
	adminConnection actionsServiceAdminConnection,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	requestURL, err := url.JoinPath(adminConnection.URL, scaleSetEndpoint)
	if err != nil {
		return nil, fmt.Errorf("build runner scale set list url: %w", err)
	}
	parsedURL, err := parseURL(requestURL)
	if err != nil {
		return nil, fmt.Errorf("parse runner scale set list url: %w", err)
	}
	query := parsedURL.Query()
	query.Set("runnerGroupId", fmt.Sprintf("%d", runnerGroupID))
	query.Set("api-version", "6.0-preview")
	parsedURL.RawQuery = query.Encode()

	request, err := newHTTPRequest(ctx, http.MethodGet, parsedURL.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create runner scale set list request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+adminConnection.Token)
	request.Header.Set("Content-Type", "application/json")

	var response runnerScaleSetsResponse
	if err := registry.doJSON(request, http.StatusOK, &response); err != nil {
		return nil, fmt.Errorf("list runner scale sets: %w", err)
	}
	return response.RunnerScaleSets, nil
}

func (registry githubScaleSetRegistry) doJSON(request *http.Request, expectedStatus int, target any) (err error) {
	response, err := registry.httpClient.Do(request) //nolint:gosec // The URL is the GitHub Actions service URL returned by GitHub's runner-registration broker.
	if err != nil {
		return fmt.Errorf("issue request: %w", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close response body: %w", closeErr)
		}
	}()
	if response.StatusCode != expectedStatus {
		return fmt.Errorf("unexpected status %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

type repositoryTarget struct {
	URL        *url.URL
	Owner      string
	Repository string
}

func parseRepositoryTarget(repositoryURL string) (repositoryTarget, error) {
	parsedURL, err := url.Parse(strings.TrimRight(repositoryURL, "/"))
	if err != nil {
		return repositoryTarget{}, fmt.Errorf("parse repository url: %w", err)
	}
	pathParts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
	if len(pathParts) != 2 || pathParts[0] == "" || pathParts[1] == "" {
		return repositoryTarget{}, errors.New("repository url must be https://github.com/OWNER/REPO")
	}
	return repositoryTarget{URL: parsedURL, Owner: pathParts[0], Repository: pathParts[1]}, nil
}

func repositoryOwnerAndName(repositoryURL string) (owner string, repository string, err error) {
	target, err := parseRepositoryTarget(repositoryURL)
	if err != nil {
		return "", "", err
	}
	return target.Owner, target.Repository, nil
}

func apiURL(repositoryURL string, path string) (string, error) {
	parsedURL, err := url.Parse(repositoryURL)
	if err != nil {
		return "", fmt.Errorf("parse repository url: %w", err)
	}
	return apiURLFromParsed(parsedURL, path), nil
}

func apiURLFromParsed(parsedURL *url.URL, path string) string {
	apiHost := parsedURL.Host
	apiPath := path
	if strings.EqualFold(parsedURL.Host, "github.com") || strings.EqualFold(parsedURL.Host, "www.github.com") {
		apiHost = "api.github.com"
	} else {
		apiPath = "/api/v3" + path
	}
	return (&url.URL{Scheme: parsedURL.Scheme, Host: apiHost, Path: apiPath}).String()
}

func runnerRegistrationURL(repositoryURL string) (string, error) {
	parsedURL, err := url.Parse(repositoryURL)
	if err != nil {
		return "", fmt.Errorf("parse repository url: %w", err)
	}
	return (&url.URL{Scheme: parsedURL.Scheme, Host: parsedURL.Host, Path: "/actions/runner-registration"}).String(), nil
}
