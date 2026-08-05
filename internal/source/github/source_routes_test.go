package github_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestSourceReplaceRoutesAtomicallyReplacesAndRemovesRoutes(t *testing.T) {
	t.Parallel()

	oldLeader := &leaderSessionFake{key: sessionKey("octo/old")}
	oldMember := &bootstrapClientFake{content: []byte("old")}
	source := sourceWithRoutes(t, oldLeader, oldMember)
	newLeader := &leaderSessionFake{key: sessionKey("octo/new")}
	newMember := &bootstrapClientFake{content: []byte("new")}

	if err := source.ReplaceRoutes(
		[]githubsource.LeaderSession{newLeader},
		[]githubsource.MemberRoute{memberRoute("octo/new", newMember)},
	); err != nil {
		t.Fatalf("replace routes: %v", err)
	}

	assertJobRejected(t, source, sourceJob(t, "octo/old", core.RunnerPool("linux"), 51))
	newJob := sourceJob(t, "octo/new", core.RunnerPool("linux"), 52)
	if err := source.Acquire(context.Background(), newJob); err != nil {
		t.Fatalf("acquire replacement route: %v", err)
	}
	if _, err := source.GenerateBootstrap(context.Background(), newJob, "runner-52"); err != nil {
		t.Fatalf("generate replacement bootstrap: %v", err)
	}
	if len(oldLeader.acquired) != 0 || oldMember.calls != 0 || len(newLeader.acquired) != 1 || newMember.calls != 1 {
		t.Fatalf("route calls: old leader=%#v old member=%d new leader=%#v new member=%d",
			oldLeader.acquired, oldMember.calls, newLeader.acquired, newMember.calls)
	}
}

func TestSourceReplaceRoutesPreservesPreviousSnapshotWhenValidationFails(t *testing.T) {
	t.Parallel()

	leader := &leaderSessionFake{key: sessionKey("octo/repo")}
	member := &bootstrapClientFake{content: []byte("opaque")}
	source := sourceWithRoutes(t, leader, member)

	err := source.ReplaceRoutes(
		[]githubsource.LeaderSession{&leaderSessionFake{key: sessionKey("octo/new")}},
		[]githubsource.MemberRoute{{Repository: "octo/new"}},
	)
	if err == nil {
		t.Fatal("ReplaceRoutes() error = nil, want invalid replacement failure")
	}

	job := sourceJob(t, "octo/repo", core.RunnerPool("linux"), 53)
	if err := source.Acquire(context.Background(), job); err != nil {
		t.Fatalf("acquire preserved route: %v", err)
	}
	if _, err := source.GenerateBootstrap(context.Background(), job, "runner-53"); err != nil {
		t.Fatalf("generate preserved bootstrap: %v", err)
	}
}

func TestSourceRoutesAreSafeDuringConcurrentReplacement(t *testing.T) {
	t.Parallel()

	firstLeader := &concurrentLeaderSession{key: sessionKey("octo/repo")}
	secondLeader := &concurrentLeaderSession{key: sessionKey("octo/repo")}
	firstMember := &concurrentBootstrapClient{}
	secondMember := &concurrentBootstrapClient{}
	source := sourceWithConcurrentRoutes(t, firstLeader, firstMember)
	job := sourceJob(t, "octo/repo", core.RunnerPool("linux"), 54)
	runConcurrentRouteOperations(t, source, job, firstLeader, secondLeader, firstMember, secondMember)
	assertConcurrentRouteCalls(t, firstLeader, secondLeader, firstMember, secondMember)
}

func runConcurrentRouteOperations(
	t *testing.T,
	source *githubsource.Source,
	job core.Job,
	firstLeader, secondLeader *concurrentLeaderSession,
	firstMember, secondMember *concurrentBootstrapClient,
) {
	t.Helper()
	const iterations = 100
	start := make(chan struct{})
	errorsFound := make(chan error, iterations*3)
	var workers sync.WaitGroup
	workers.Add(3)
	go concurrentlyAcquire(t.Context(), source, job, start, errorsFound, &workers)
	go concurrentlyGenerate(t.Context(), source, job, start, errorsFound, &workers)
	go concurrentlyReplace(source, firstLeader, secondLeader, firstMember, secondMember, start, errorsFound, &workers)
	close(start)
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent route operation: %v", err)
		}
	}
}

func assertConcurrentRouteCalls(
	t *testing.T,
	firstLeader, secondLeader *concurrentLeaderSession,
	firstMember, secondMember *concurrentBootstrapClient,
) {
	t.Helper()
	const iterations = 100
	if firstLeader.calls.Load()+secondLeader.calls.Load() != iterations {
		t.Fatalf("acquisition calls = %d, want %d", firstLeader.calls.Load()+secondLeader.calls.Load(), iterations)
	}
	if firstMember.calls.Load()+secondMember.calls.Load() != iterations {
		t.Fatalf("bootstrap calls = %d, want %d", firstMember.calls.Load()+secondMember.calls.Load(), iterations)
	}
}

func concurrentlyAcquire(
	ctx context.Context,
	source *githubsource.Source,
	job core.Job,
	start <-chan struct{},
	errorsFound chan<- error,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	<-start
	for range 100 {
		errorsFound <- source.Acquire(ctx, job)
	}
}

func concurrentlyGenerate(
	ctx context.Context,
	source *githubsource.Source,
	job core.Job,
	start <-chan struct{},
	errorsFound chan<- error,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	<-start
	for range 100 {
		_, err := source.GenerateBootstrap(ctx, job, "runner")
		errorsFound <- err
	}
}

func concurrentlyReplace(
	source *githubsource.Source,
	firstLeader, secondLeader *concurrentLeaderSession,
	firstMember, secondMember *concurrentBootstrapClient,
	start <-chan struct{},
	errorsFound chan<- error,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	<-start
	for iteration := range 100 {
		leader, member := firstLeader, firstMember
		if iteration%2 == 0 {
			leader, member = secondLeader, secondMember
		}
		errorsFound <- source.ReplaceRoutes(
			[]githubsource.LeaderSession{leader},
			[]githubsource.MemberRoute{memberRoute("octo/repo", member)},
		)
	}
}

func sourceWithRoutes(
	t *testing.T,
	leader githubsource.LeaderSession,
	member githubsource.BootstrapClient,
) *githubsource.Source {
	t.Helper()
	source, err := githubsource.NewSource(
		[]githubsource.LeaderSession{leader},
		[]githubsource.MemberRoute{memberRoute(leader.Key().Repository, member)},
	)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	return source
}

func sourceWithConcurrentRoutes(
	t *testing.T,
	leader *concurrentLeaderSession,
	member *concurrentBootstrapClient,
) *githubsource.Source {
	t.Helper()
	return sourceWithRoutes(t, leader, member)
}

func memberRoute(repository string, client githubsource.BootstrapClient) githubsource.MemberRoute {
	return githubsource.MemberRoute{
		Repository: repository, RunnerPool: core.RunnerPool("linux"), Client: client,
		Registration: transport.Registration{ID: 17, Name: "linux"},
	}
}

func sessionKey(repository string) transport.SessionKey {
	return transport.SessionKey{Repository: repository, RunnerPool: "linux"}
}

type concurrentLeaderSession struct {
	key   transport.SessionKey
	calls atomic.Int64
}

func (session *concurrentLeaderSession) Key() transport.SessionKey { return session.key }

func (session *concurrentLeaderSession) AcquireRequest(context.Context, int64) error {
	session.calls.Add(1)
	return nil
}

type concurrentBootstrapClient struct {
	calls atomic.Int64
}

func (client *concurrentBootstrapClient) GenerateBootstrap(
	context.Context,
	transport.Registration,
	string,
) ([]byte, error) {
	client.calls.Add(1)
	return []byte("opaque"), nil
}
