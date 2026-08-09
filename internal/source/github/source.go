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

// BootstrapClient generates runner bootstrap data through an ordinary
// repository client; it does not require the leader's message session.
type BootstrapClient interface {
	GenerateBootstrap(
		ctx context.Context,
		registration transport.Registration,
		runnerName core.RunnerName,
	) ([]byte, error)
	CleanupRunner(ctx context.Context, runnerName core.RunnerName) error
}

// CleanupBootstrap removes the exact JIT registration owned by an unstarted
// provisioning attempt.
func (source *Source) CleanupBootstrap(ctx context.Context, job core.Job, runnerName core.RunnerName) error {
	key, err := sourceIdentityForJob(job)
	if err != nil {
		return err
	}
	source.routesMu.RLock()
	route, found := source.routes[key]
	source.routesMu.RUnlock()
	if !found {
		return fmt.Errorf("GitHub member route for repository %q and Runner Pool %q was not found", key.repository, key.runnerPool)
	}
	if err = route.client.CleanupRunner(ctx, runnerName); err != nil {
		return fmt.Errorf("cleanup GitHub runner %q for repository %q and Runner Pool %q: %w",
			runnerName, key.repository, key.runnerPool, err)
	}
	return nil
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

// Source routes bootstrap generation to member-local repository clients.
// GitHub request acquisition belongs to leader polling adapters and never
// passes through this member-facing source.
type Source struct {
	routesMu sync.RWMutex
	routes   map[routeKey]memberRoute
}

type bootstrapDiagnosticError struct{ cause error }

func (failure bootstrapDiagnosticError) Error() string                 { return failure.cause.Error() }
func (failure bootstrapDiagnosticError) Unwrap() error                 { return failure.cause }
func (failure bootstrapDiagnosticError) OperationalDiagnostic() string { return failure.cause.Error() }

// NewSource builds the GitHub bootstrap router. Duplicate or incomplete routes
// are rejected rather than resolved by registration order.
func NewSource(routes []MemberRoute) (*Source, error) {
	source := &Source{}
	if err := source.ReplaceRoutes(routes); err != nil {
		return nil, err
	}
	return source, nil
}

// ReplaceRoutes atomically replaces every bootstrap route after validating a
// complete immutable snapshot. Calls already in progress retain their selected
// route; later calls observe the new snapshot.
func (source *Source) ReplaceRoutes(routes []MemberRoute) error {
	snapshot, err := buildRouteSnapshot(routes)
	if err != nil {
		return err
	}
	source.routesMu.Lock()
	source.routes = snapshot
	source.routesMu.Unlock()
	return nil
}

func buildRouteSnapshot(routes []MemberRoute) (map[routeKey]memberRoute, error) {
	snapshot := make(map[routeKey]memberRoute, len(routes))
	for _, route := range routes {
		key := routeKey{repository: route.Repository, runnerPool: route.RunnerPool}
		if key.repository == "" || key.runnerPool == "" || route.Client == nil {
			return nil, errors.New("GitHub member route repository, Runner Pool, and client are required")
		}
		if route.Registration.ID <= 0 || route.Registration.Name != string(route.RunnerPool) {
			return nil, errors.New("GitHub member route registration must match its Runner Pool")
		}
		if _, exists := snapshot[key]; exists {
			return nil, fmt.Errorf(
				"duplicate GitHub member route for repository %q and Runner Pool %q",
				key.repository,
				key.runnerPool,
			)
		}
		snapshot[key] = memberRoute{client: route.Client, registration: route.Registration}
	}
	return snapshot, nil
}

// GenerateBootstrap routes a job to the member-local ordinary repository
// client and wraps the returned opaque JIT configuration for execution.
func (source *Source) GenerateBootstrap(
	ctx context.Context,
	job core.Job,
	runnerName core.RunnerName,
) (execution.Bootstrap, error) {
	key, err := sourceIdentityForJob(job)
	if err != nil {
		return execution.Bootstrap{}, err
	}
	source.routesMu.RLock()
	route, found := source.routes[key]
	source.routesMu.RUnlock()
	if !found {
		return execution.Bootstrap{}, fmt.Errorf(
			"GitHub member route for repository %q and Runner Pool %q was not found", key.repository, key.runnerPool,
		)
	}
	content, err := route.client.GenerateBootstrap(ctx, route.registration, runnerName)
	if err != nil {
		return execution.Bootstrap{}, bootstrapDiagnosticError{cause: fmt.Errorf(
			"generate GitHub bootstrap for repository %q and Runner Pool %q: %w", key.repository, key.runnerPool, err)}
	}
	if len(content) == 0 {
		return execution.Bootstrap{}, errors.New("GitHub bootstrap content is empty")
	}
	return execution.NewBootstrap(content), nil
}

func sourceIdentityForJob(job core.Job) (routeKey, error) {
	if job.Source.Adapter != AdapterName {
		return routeKey{}, fmt.Errorf("job source adapter %q is not GitHub", job.Source.Adapter)
	}
	identity, err := DecodeSourceID(job.Source.ID)
	if err != nil {
		return routeKey{}, fmt.Errorf("decode source identity for job %s: %w", job.ID, err)
	}
	if identity.RunnerPool != job.RunnerPool {
		return routeKey{}, fmt.Errorf(
			"job Runner Pool %q does not match encoded GitHub Runner Pool %q", job.RunnerPool, identity.RunnerPool,
		)
	}
	return routeKey{repository: identity.Repository, runnerPool: identity.RunnerPool}, nil
}
