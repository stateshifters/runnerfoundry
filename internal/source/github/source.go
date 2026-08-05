package github

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

// LeaderSession acquires GitHub requests through the leader-owned message
// session for one repository and Runner Pool.
type LeaderSession interface {
	Key() transport.SessionKey
	AcquireRequest(ctx context.Context, requestID int64) error
}

// BootstrapClient generates runner bootstrap data through an ordinary
// repository client; it does not require the leader's message session.
type BootstrapClient interface {
	GenerateBootstrap(
		ctx context.Context,
		registration transport.Registration,
		runnerName string,
	) ([]byte, error)
}

// MemberRoute identifies the ordinary repository client and runner scale-set
// registration used by a member to generate bootstrap data.
type MemberRoute struct {
	Repository   string
	RunnerPool   core.RunnerPool
	Client       BootstrapClient
	Registration transport.Registration
}

type routeKey struct {
	repository string
	runnerPool core.RunnerPool
}

type memberRoute struct {
	client       BootstrapClient
	registration transport.Registration
}

type routeSnapshot struct {
	leaderRoutes map[routeKey]LeaderSession
	memberRoutes map[routeKey]memberRoute
}

// Source routes source operations by the repository and Runner Pool encoded
// in each job's GitHub source identity. Leader acquisition and member
// bootstrap generation are deliberately separate: workers never reuse or
// assume ownership of the leader's message session.
type Source struct {
	routesMu     sync.RWMutex
	leaderRoutes map[routeKey]LeaderSession
	memberRoutes map[routeKey]memberRoute
}

// NewSource builds the GitHub source router from the leader sessions and
// member-local bootstrap routes available to this process. Duplicate or
// incomplete routes are rejected rather than resolved by registration order.
func NewSource(leaderSessions []LeaderSession, memberRoutes []MemberRoute) (*Source, error) {
	source := &Source{}
	if err := source.ReplaceRoutes(leaderSessions, memberRoutes); err != nil {
		return nil, err
	}
	return source, nil
}

// ReplaceRoutes atomically replaces every leader and member route. It fully
// validates a new immutable snapshot before acquiring the write lock, so a
// failed replacement leaves the active routes unchanged. Calls already in
// progress retain their selected route; later calls observe the new snapshot.
func (source *Source) ReplaceRoutes(leaderSessions []LeaderSession, memberRoutes []MemberRoute) error {
	snapshot, err := buildRouteSnapshot(leaderSessions, memberRoutes)
	if err != nil {
		return err
	}
	source.routesMu.Lock()
	source.leaderRoutes = snapshot.leaderRoutes
	source.memberRoutes = snapshot.memberRoutes
	source.routesMu.Unlock()
	return nil
}

func buildRouteSnapshot(leaderSessions []LeaderSession, memberRoutes []MemberRoute) (routeSnapshot, error) {
	snapshot := routeSnapshot{
		leaderRoutes: make(map[routeKey]LeaderSession, len(leaderSessions)),
		memberRoutes: make(map[routeKey]memberRoute, len(memberRoutes)),
	}
	for _, session := range leaderSessions {
		if err := snapshot.addLeaderRoute(session); err != nil {
			return routeSnapshot{}, err
		}
	}
	for _, route := range memberRoutes {
		if err := snapshot.addMemberRoute(route); err != nil {
			return routeSnapshot{}, err
		}
	}
	return snapshot, nil
}

func (snapshot routeSnapshot) addLeaderRoute(session LeaderSession) error {
	if session == nil {
		return errors.New("GitHub leader session is required")
	}
	sessionKey := session.Key()
	key := routeKey{repository: sessionKey.Repository, runnerPool: core.RunnerPool(sessionKey.RunnerPool)}
	if key.repository == "" || key.runnerPool == "" {
		return errors.New("GitHub leader session repository and Runner Pool are required")
	}
	if _, exists := snapshot.leaderRoutes[key]; exists {
		return fmt.Errorf("duplicate GitHub leader route for repository %q and Runner Pool %q", key.repository, key.runnerPool)
	}
	snapshot.leaderRoutes[key] = session
	return nil
}

func (snapshot routeSnapshot) addMemberRoute(route MemberRoute) error {
	key := routeKey{repository: route.Repository, runnerPool: route.RunnerPool}
	if key.repository == "" || key.runnerPool == "" || route.Client == nil {
		return errors.New("GitHub member route repository, Runner Pool, and client are required")
	}
	if route.Registration.ID <= 0 || route.Registration.Name != string(route.RunnerPool) {
		return errors.New("GitHub member route registration must match its Runner Pool")
	}
	if _, exists := snapshot.memberRoutes[key]; exists {
		return fmt.Errorf("duplicate GitHub member route for repository %q and Runner Pool %q", key.repository, key.runnerPool)
	}
	snapshot.memberRoutes[key] = memberRoute{client: route.Client, registration: route.Registration}
	return nil
}

// Acquire routes a job to the leader session that owns its repository and
// Runner Pool, then acquires only its decoded numeric GitHub request ID.
func (source *Source) Acquire(ctx context.Context, job core.Job) error {
	identity, key, err := sourceIdentityForJob(job)
	if err != nil {
		return err
	}
	source.routesMu.RLock()
	session, found := source.leaderRoutes[key]
	source.routesMu.RUnlock()
	if !found {
		return fmt.Errorf("GitHub leader route for repository %q and Runner Pool %q was not found", key.repository, key.runnerPool)
	}
	if err := session.AcquireRequest(ctx, identity.RequestID); err != nil {
		return fmt.Errorf("acquire GitHub request %d for repository %q and Runner Pool %q: %w",
			identity.RequestID, key.repository, key.runnerPool, err)
	}
	return nil
}

// GenerateBootstrap routes a job to the member-local ordinary repository
// client and wraps the returned opaque JIT configuration for execution.
func (source *Source) GenerateBootstrap(
	ctx context.Context,
	job core.Job,
	runnerName string,
) (execution.Bootstrap, error) {
	_, key, err := sourceIdentityForJob(job)
	if err != nil {
		return execution.Bootstrap{}, err
	}
	source.routesMu.RLock()
	route, found := source.memberRoutes[key]
	source.routesMu.RUnlock()
	if !found {
		return execution.Bootstrap{}, fmt.Errorf(
			"GitHub member route for repository %q and Runner Pool %q was not found", key.repository, key.runnerPool,
		)
	}
	content, err := route.client.GenerateBootstrap(ctx, route.registration, runnerName)
	if err != nil {
		return execution.Bootstrap{}, fmt.Errorf(
			"generate GitHub bootstrap for repository %q and Runner Pool %q: %w", key.repository, key.runnerPool, err,
		)
	}
	if len(content) == 0 {
		return execution.Bootstrap{}, errors.New("GitHub bootstrap content is empty")
	}
	return execution.NewBootstrap(content), nil
}

// ReportResult applies GitHub's result-delivery policy without requiring a
// repository route or external API call.
func (source *Source) ReportResult(
	_ context.Context,
	job core.Job,
	result core.Result,
) (core.ResultDelivery, error) {
	return resultDelivery(job, result), nil
}

func sourceIdentityForJob(job core.Job) (SourceIdentity, routeKey, error) {
	if job.Source.Adapter != AdapterName {
		return SourceIdentity{}, routeKey{}, fmt.Errorf("job source adapter %q is not GitHub", job.Source.Adapter)
	}
	identity, err := DecodeSourceID(job.Source.ID)
	if err != nil {
		return SourceIdentity{}, routeKey{}, fmt.Errorf("decode source identity for job %s: %w", job.ID, err)
	}
	if identity.RunnerPool != job.RunnerPool {
		return SourceIdentity{}, routeKey{}, fmt.Errorf(
			"job Runner Pool %q does not match encoded GitHub Runner Pool %q", job.RunnerPool, identity.RunnerPool,
		)
	}
	return identity, routeKey{repository: identity.Repository, runnerPool: identity.RunnerPool}, nil
}

func resultDelivery(job core.Job, result core.Result) core.ResultDelivery {
	if job.Source.Adapter != AdapterName || result.Origin != core.ResultOriginRunner {
		return core.ResultDeliveryUnsupported
	}
	return core.ResultDeliveryRunnerManaged
}
