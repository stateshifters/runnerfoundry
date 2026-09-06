package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

	upstream "github.com/actions/scaleset"
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
	Session() upstream.RunnerScaleSetSession
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
	GetRunnerByName(ctx context.Context, runnerName string) (*upstream.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

type repositoryAPI interface {
	registrationAPI
	jitAPI
	registryAPI
}

type sessionClientAPI interface {
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
	registrations    registrationAPI
	openSession      func(context.Context, int, string, ...upstream.HTTPOption) (messageSessionAPI, error)
	jit              jitAPI
	registry         registryAPI
	repositoryURL    string
	transportFactory TransportFactory
	gateOnce         sync.Once
	repositoryGate   chan struct{}
}

func NewClient(repository Repository, credentials Credentials, factory TransportFactory) (*Client, error) {
	api, err := newSDKClient(repository, credentials, factory)
	if err != nil {
		return nil, err
	}
	sessionAPI, err := newSDKClient(repository, credentials, factory)
	if err != nil {
		return nil, err
	}
	client := clientFromUpstream(repository.URL, api, sessionAPI, factory)
	return client, nil
}

func clientFromUpstream(repositoryURL string, api repositoryAPI, sessionAPI sessionClientAPI, factory TransportFactory) *Client {
	return &Client{
		registrations: api,
		openSession: func(ctx context.Context, runnerScaleSetID int, owner string, options ...upstream.HTTPOption) (messageSessionAPI, error) {
			session, sessionErr := sessionAPI.MessageSessionClient(ctx, runnerScaleSetID, owner, options...)
			if sessionErr != nil {
				return nil, fmt.Errorf("create upstream message session: %w", sessionErr)
			}
			return session, nil
		},
		jit:              api,
		registry:         api,
		repositoryURL:    repositoryURL,
		transportFactory: factory,
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
	if err := client.lockRepository(ctx); err != nil {
		return Registration{}, err
	}
	defer client.unlockRepository()
	runnerGroupID, err := client.findRunnerGroup(ctx, runnerGroupName)
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

// ResolveRunnerGroup resolves a configured GitHub runner-group name to its
// stable numeric identity for registration admission checks.
func (client *Client) ResolveRunnerGroup(ctx context.Context, runnerGroupName string) (RunnerGroupID, error) {
	if client == nil || client.registrations == nil {
		return 0, errors.New("GitHub registration API is required")
	}
	if err := client.lockRepository(ctx); err != nil {
		return 0, err
	}
	defer client.unlockRepository()
	return client.findRunnerGroup(ctx, runnerGroupName)
}

func (client *Client) findRunnerGroup(ctx context.Context, runnerGroupName string) (RunnerGroupID, error) {
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

func (client *Client) lockRepository(ctx context.Context) error {
	client.gateOnce.Do(func() { client.repositoryGate = make(chan struct{}, 1) })
	select {
	case client.repositoryGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			client.unlockRepository()
			return fmt.Errorf("acquire GitHub repository operation: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for GitHub repository operation: %w", ctx.Err())
	}
}

func (client *Client) unlockRepository() { <-client.repositoryGate }
