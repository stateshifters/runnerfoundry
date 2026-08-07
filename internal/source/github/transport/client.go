package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	upstream "github.com/actions/scaleset"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/meza/RunnerFoundry/internal/buildinfo"
)

type Registration struct {
	ID            ScaleSetID
	Name          string
	RunnerGroupID RunnerGroupID
}

type registrationAPI interface {
	GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error)
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, name string) (*upstream.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, runnerScaleSet *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error)
}

type messageSessionAPI interface {
	GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error)
	DeleteMessage(ctx context.Context, messageID int) error
	AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error)
	Close(ctx context.Context) error
}

type jitAPI interface {
	GenerateJitRunnerConfig(
		ctx context.Context,
		setting *upstream.RunnerScaleSetJitRunnerSetting,
		runnerScaleSetID int,
	) (*upstream.RunnerScaleSetJitRunnerConfig, error)
}

type upstreamClientAPI interface {
	registrationAPI
	jitAPI
	registryAPI
	MessageSessionClient(
		ctx context.Context,
		runnerScaleSetID int,
		owner string,
		options ...upstream.HTTPOption,
	) (*upstream.MessageSessionClient, error)
}

type registryAPI interface {
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]upstream.RunnerScaleSet, error)
	DeleteRunnerScaleSet(ctx context.Context, runnerScaleSetID int) error
}

// Client owns GitHub Actions runner scale-set registration, message sessions,
// and runner bootstrap operations for one repository installation.
type Client struct {
	registrations registrationAPI
	openSession   func(context.Context, int, string) (messageSessionAPI, error)
	jit           jitAPI
	registry      registryAPI
	repositoryURL string
}

// NewClient creates an authenticated GitHub Actions client for one repository.
// All network traffic uses baseTransport; callers may pass nil for the process default.
func NewClient(repository Repository, credentials Credentials, baseTransport http.RoundTripper) (*Client, error) {
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient.Transport = baseTransport
	retryClient.Logger = nil
	api, err := upstream.NewClientWithGitHubApp(upstream.ClientWithGitHubAppConfig{
		GitHubConfigURL: repository.URL,
		GitHubAppAuth: upstream.GitHubAppAuth{
			ClientID:       strconv.FormatInt(credentials.AppID, 10),
			InstallationID: int64(repository.InstallationID),
			PrivateKey:     credentials.PrivateKey,
		},
		SystemInfo: upstream.SystemInfo{System: "runnerfoundry", Version: buildinfo.Version()},
	}, upstream.WithRetryableHTTPClint(retryClient))
	if err != nil {
		return nil, fmt.Errorf("create GitHub Actions scale-set client: %w", err)
	}
	client := clientFromUpstream(repository.URL, api)
	return client, nil
}

func clientFromUpstream(repositoryURL string, api upstreamClientAPI) *Client {
	return &Client{
		registrations: api,
		openSession: func(ctx context.Context, runnerScaleSetID int, owner string) (messageSessionAPI, error) {
			session, sessionErr := api.MessageSessionClient(ctx, runnerScaleSetID, owner)
			if sessionErr != nil {
				return nil, fmt.Errorf("create upstream message session: %w", sessionErr)
			}
			return adaptUpstreamMessageSession(session), nil
		},
		jit:           api,
		registry:      api,
		repositoryURL: repositoryURL,
	}
}

// EnsureRunnerPool returns the existing GitHub runner scale set for the named
// Runner Pool, or creates it with a matching system label and disabled updates.
func (client *Client) EnsureRunnerPool(
	ctx context.Context,
	runnerPoolName string,
	runnerGroupName string,
) (Registration, error) {
	if client == nil || client.registrations == nil {
		return Registration{}, errors.New("GitHub registration API is required")
	}
	runnerGroupID, err := client.runnerGroupID(ctx, runnerGroupName)
	if err != nil {
		return Registration{}, err
	}
	runnerScaleSet, err := client.registrations.GetRunnerScaleSet(ctx, int(runnerGroupID), runnerPoolName)
	if err != nil {
		return Registration{}, fmt.Errorf("get runner scale set %q: %w", runnerPoolName, err)
	}
	if runnerScaleSet == nil {
		runnerScaleSet, err = client.createRunnerScaleSet(ctx, runnerPoolName, runnerGroupID)
	}
	if err != nil {
		return Registration{}, err
	}
	if runnerScaleSet == nil {
		return Registration{}, fmt.Errorf("ensure runner scale set %q returned no registration", runnerPoolName)
	}
	return Registration{
		ID: ScaleSetID(runnerScaleSet.ID), Name: runnerScaleSet.Name, RunnerGroupID: RunnerGroupID(runnerScaleSet.RunnerGroupID),
	}, nil
}

func (client *Client) runnerGroupID(ctx context.Context, runnerGroupName string) (RunnerGroupID, error) {
	runnerGroup, err := client.registrations.GetRunnerGroupByName(ctx, runnerGroupName)
	if err != nil {
		return 0, fmt.Errorf("get runner group %q: %w", runnerGroupName, err)
	}
	if runnerGroup == nil {
		return 0, fmt.Errorf("runner group %q was not found", runnerGroupName)
	}
	return RunnerGroupID(runnerGroup.ID), nil
}

func (client *Client) createRunnerScaleSet(
	ctx context.Context,
	runnerPoolName string,
	runnerGroupID RunnerGroupID,
) (*upstream.RunnerScaleSet, error) {
	runnerScaleSet, err := client.registrations.CreateRunnerScaleSet(ctx, &upstream.RunnerScaleSet{
		Name:          runnerPoolName,
		RunnerGroupID: int(runnerGroupID),
		Labels:        []upstream.Label{{Name: runnerPoolName, Type: "System"}},
		RunnerSetting: upstream.RunnerSetting{DisableUpdate: true},
	})
	if err != nil {
		return nil, fmt.Errorf("create runner scale set %q: %w", runnerPoolName, err)
	}
	return runnerScaleSet, nil
}
