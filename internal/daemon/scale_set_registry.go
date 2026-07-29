package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

const scaleSetEndpoint = "_apis/runtime/runnerscalesets"
const githubScaleSetRegistryTimeout = 30 * time.Second

type scaleSetRegistry interface {
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
}

type githubScaleSetRegistry struct {
	repositoryURL      string
	installationClient *http.Client
	protocolClient     *http.Client
}

type githubRegistrationToken struct {
	Token string `json:"token"`
}

type actionsServiceAdminConnection struct {
	URL   actionsServiceURL
	Token string
}

type actionsServiceAdminConnectionResponse struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type runnerScaleSetsResponse struct {
	Count           int                       `json:"count"`
	RunnerScaleSets []upstream.RunnerScaleSet `json:"value"`
}

func newGitHubScaleSetRegistry(
	repositoryURL string,
	installationID githubapp.InstallationID,
	credentials config.GitHubAppCredentials,
) (githubScaleSetRegistry, error) {
	return newGitHubScaleSetRegistryWithTransport(repositoryURL, installationID, credentials, http.DefaultTransport)
}

func newGitHubScaleSetRegistryWithTransport(
	repositoryURL string,
	installationID githubapp.InstallationID,
	credentials config.GitHubAppCredentials,
	protocolTransport http.RoundTripper,
) (githubScaleSetRegistry, error) {
	installationTransport, err := ghinstallation.New(
		protocolTransport, credentials.AppID, int64(installationID), []byte(credentials.PrivateKey),
	)
	if err != nil {
		return githubScaleSetRegistry{}, fmt.Errorf("create installation transport: %w", err)
	}
	return githubScaleSetRegistry{
		repositoryURL: repositoryURL,
		installationClient: &http.Client{
			Timeout: githubScaleSetRegistryTimeout, Transport: installationTransport,
		},
		protocolClient: &http.Client{
			Timeout: githubScaleSetRegistryTimeout, Transport: protocolTransport,
		},
	}, nil
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
	responseBody, err := registry.requestRunnerRegistrationToken(ctx, target)
	if err != nil {
		return "", err
	}
	var registrationToken githubRegistrationToken
	if decodeErr := json.NewDecoder(bytes.NewReader(responseBody)).Decode(&registrationToken); decodeErr != nil {
		return "", fmt.Errorf("get runner registration token: decode response: %w", decodeErr)
	}
	if registrationToken.Token == "" {
		return "", errors.New("runner registration token response missing token")
	}
	return registrationToken.Token, nil
}

func (registry githubScaleSetRegistry) requestRunnerRegistrationToken(
	ctx context.Context,
	target repositoryTarget,
) ([]byte, error) {
	requestURL := apiURLValueFromParsed(target.URL,
		fmt.Sprintf("/repos/%s/%s/actions/runners/registration-token", target.Owner, target.Repository))
	request := (&http.Request{
		Method: http.MethodPost,
		URL:    requestURL,
		Header: make(http.Header),
		Body:   http.NoBody,
	}).WithContext(ctx)
	request.Header.Set("Accept", "application/vnd.github+json")

	client := registry.httpClientForOrigin(registry.installationClient, requestURL)
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get runner registration token: issue request: %w", err)
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("get runner registration token: unexpected status %s", response.Status)
	}
	if readErr != nil {
		return nil, fmt.Errorf("get runner registration token: read response: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("get runner registration token: close response body: %w", closeErr)
	}
	return responseBody, nil
}

func (registry githubScaleSetRegistry) actionsServiceAdminConnection(
	ctx context.Context,
	registrationToken string,
) (actionsServiceAdminConnection, error) {
	repository, err := parseRepositoryTarget(registry.repositoryURL)
	if err != nil {
		return actionsServiceAdminConnection{}, err
	}
	response, err := registry.requestActionsServiceAdminConnection(ctx, registrationToken, repository)
	if err != nil {
		return actionsServiceAdminConnection{}, err
	}
	return validatedActionsServiceAdminConnection(response, repository)
}

func (registry githubScaleSetRegistry) requestActionsServiceAdminConnection(
	ctx context.Context,
	registrationToken string,
	repository repositoryTarget,
) (actionsServiceAdminConnectionResponse, error) {
	responseBody, err := registry.executeActionsServiceAdminRequest(ctx, registrationToken, repository)
	if err != nil {
		return actionsServiceAdminConnectionResponse{}, err
	}
	var response actionsServiceAdminConnectionResponse
	if decodeErr := json.NewDecoder(bytes.NewReader(responseBody)).Decode(&response); decodeErr != nil {
		return actionsServiceAdminConnectionResponse{}, fmt.Errorf("get actions service admin connection: decode response: %w", decodeErr)
	}
	return response, nil
}

func (registry githubScaleSetRegistry) executeActionsServiceAdminRequest(
	ctx context.Context,
	registrationToken string,
	repository repositoryTarget,
) ([]byte, error) {
	requestURL := apiURLValueFromParsed(repository.URL, "/actions/runner-registration")
	request := actionsServiceAdminRequest(ctx, requestURL, registry.repositoryURL, registrationToken)
	client := registry.httpClientForOrigin(registry.protocolClient, requestURL)
	httpResponse, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get actions service admin connection: issue request: %w", err)
	}
	responseBody, readErr := io.ReadAll(httpResponse.Body)
	closeErr := httpResponse.Body.Close()
	return validateActionsServiceAdminResponse(httpResponse, responseBody, readErr, closeErr)
}

func actionsServiceAdminRequest(
	ctx context.Context,
	requestURL *url.URL,
	repositoryURL string,
	registrationToken string,
) *http.Request {
	body := []byte(fmt.Sprintf(`{"url":%s,"runner_event":"register"}`, strconv.Quote(repositoryURL)))
	request := (&http.Request{
		Method: http.MethodPost, URL: requestURL, Header: make(http.Header),
		Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)),
	}).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "RemoteAuth "+registrationToken)
	return request
}

func validateActionsServiceAdminResponse(
	httpResponse *http.Response,
	responseBody []byte,
	readErr error,
	closeErr error,
) ([]byte, error) {
	if httpResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get actions service admin connection: unexpected status %s", httpResponse.Status)
	}
	if readErr != nil {
		return nil, fmt.Errorf("get actions service admin connection: read response: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("get actions service admin connection: close response body: %w", closeErr)
	}
	return responseBody, nil
}

func validatedActionsServiceAdminConnection(
	response actionsServiceAdminConnectionResponse,
	repository repositoryTarget,
) (actionsServiceAdminConnection, error) {
	if response.URL == "" || response.Token == "" {
		return actionsServiceAdminConnection{}, errors.New("actions service admin connection response missing url or token")
	}
	serviceURL, err := parseActionsServiceURL(response.URL, repository)
	if err != nil {
		return actionsServiceAdminConnection{}, err
	}
	return actionsServiceAdminConnection{URL: serviceURL, Token: response.Token}, nil
}

func (registry githubScaleSetRegistry) fetchRunnerScaleSets(
	ctx context.Context,
	adminConnection actionsServiceAdminConnection,
	runnerGroupID int,
) ([]upstream.RunnerScaleSet, error) {
	request := runnerScaleSetListRequest(ctx, adminConnection, runnerGroupID)

	client := registry.httpClientForOrigin(registry.protocolClient, adminConnection.URL.parsed)
	httpResponse, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list runner scale sets: issue request: %w", err)
	}
	responseBody, readErr := io.ReadAll(httpResponse.Body)
	closeErr := httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list runner scale sets: unexpected status %s", httpResponse.Status)
	}
	if readErr != nil {
		return nil, fmt.Errorf("list runner scale sets: read response: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("list runner scale sets: close response body: %w", closeErr)
	}
	var response runnerScaleSetsResponse
	if decodeErr := json.NewDecoder(bytes.NewReader(responseBody)).Decode(&response); decodeErr != nil {
		return nil, fmt.Errorf("list runner scale sets: decode response: %w", decodeErr)
	}
	return response.RunnerScaleSets, nil
}

func runnerScaleSetListRequest(
	ctx context.Context,
	adminConnection actionsServiceAdminConnection,
	runnerGroupID int,
) *http.Request {
	parsedURL := adminConnection.URL.parsed.JoinPath(scaleSetEndpoint)
	query := parsedURL.Query()
	query.Set("runnerGroupId", fmt.Sprintf("%d", runnerGroupID))
	query.Set("api-version", "6.0-preview")
	parsedURL.RawQuery = query.Encode()

	request := (&http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
		Header: make(http.Header),
		Body:   http.NoBody,
	}).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+adminConnection.Token)
	request.Header.Set("Content-Type", "application/json")
	return request
}

func (registry githubScaleSetRegistry) httpClientForOrigin(configuredClient *http.Client, origin *url.URL) *http.Client {
	client := *configuredClient
	client.CheckRedirect = sameOriginRedirectPolicy(origin)
	return &client
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
	if err := requireHTTPSURL(parsedURL, "repository url"); err != nil {
		return repositoryTarget{}, err
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
	if err := requireHTTPSURL(parsedURL, "repository url"); err != nil {
		return "", err
	}
	return apiURLFromParsed(parsedURL, path), nil
}

func requireHTTPSURL(parsedURL *url.URL, description string) error {
	if parsedURL.Scheme != "https" {
		return fmt.Errorf("%s must use https", description)
	}
	if parsedURL.Host == "" {
		return fmt.Errorf("%s must include a host", description)
	}
	if parsedURL.User != nil {
		return fmt.Errorf("%s must not include user information", description)
	}
	return nil
}

func apiURLFromParsed(parsedURL *url.URL, path string) string {
	return apiURLValueFromParsed(parsedURL, path).String()
}

func apiURLValueFromParsed(parsedURL *url.URL, path string) *url.URL {
	apiHost := parsedURL.Host
	apiPath := path
	if strings.EqualFold(parsedURL.Host, "github.com") || strings.EqualFold(parsedURL.Host, "www.github.com") {
		apiHost = "api.github.com"
	} else {
		apiPath = "/api/v3" + path
	}
	return &url.URL{Scheme: parsedURL.Scheme, Host: apiHost, Path: apiPath}
}

func runnerRegistrationURL(repositoryURL string) (string, error) {
	return apiURL(repositoryURL, "/actions/runner-registration")
}
