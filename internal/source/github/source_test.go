package github_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestSourceRoutesAcquisitionByRepositoryAndRunnerPool(t *testing.T) {
	t.Parallel()

	first := &leaderSessionFake{key: transport.SessionKey{Repository: "octo/first", RunnerPool: "linux"}}
	second := &leaderSessionFake{key: transport.SessionKey{Repository: "octo/second", RunnerPool: "linux"}}
	source, err := githubsource.NewSource([]githubsource.LeaderSession{first, second}, nil)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}

	job := sourceJob(t, "octo/second", core.RunnerPool("linux"), 42)
	if err := source.Acquire(context.Background(), job); err != nil {
		t.Fatalf("acquire routed request: %v", err)
	}
	if len(first.acquired) != 0 || !reflect.DeepEqual(second.acquired, []int64{42}) {
		t.Fatalf("acquisitions: first=%#v second=%#v", first.acquired, second.acquired)
	}
}

func TestSourceRoutesBootstrapToMemberClientWithoutLeaderSession(t *testing.T) {
	t.Parallel()

	first := &bootstrapClientFake{content: []byte("wrong")}
	second := &bootstrapClientFake{content: []byte("opaque-jit")}
	source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{
		{
			Repository: "octo/first", RunnerPool: core.RunnerPool("linux"),
			Client: first, Registration: transport.Registration{ID: 11, Name: "linux"},
		},
		{
			Repository: "octo/second", RunnerPool: core.RunnerPool("linux"),
			Client: second, Registration: transport.Registration{ID: 17, Name: "linux"},
		},
	})
	job := sourceJob(t, "octo/second", core.RunnerPool("linux"), 43)
	bootstrap, err := source.GenerateBootstrap(context.Background(), job, "runner-43")
	if err != nil {
		t.Fatalf("generate routed bootstrap: %v", err)
	}
	if first.calls != 0 || second.calls != 1 || second.registration.ID != 17 || second.runnerName != "runner-43" {
		t.Fatalf("bootstrap routes: first=%d second=%d registration=%#v runner=%q",
			first.calls, second.calls, second.registration, second.runnerName)
	}
	if !reflect.DeepEqual(bootstrap, execution.NewBootstrap([]byte("opaque-jit"))) {
		t.Fatalf("bootstrap = %#v, want wrapped opaque content", bootstrap)
	}
}

func TestSourceRejectsMismatchedOrMissingRoutes(t *testing.T) {
	t.Parallel()

	source, leader, member := newRoutingFixture(t)

	tests := map[string]core.Job{
		"foreign adapter": {
			Source: core.SourceReference{Adapter: "gitlab", ID: "opaque"}, RunnerPool: core.RunnerPool("linux"),
		},
		"malformed identity": {
			Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: "malformed"}, RunnerPool: core.RunnerPool("linux"),
		},
		"pool mismatch": sourceJob(t, "octo/repo", core.RunnerPool("windows"), 44),
		"missing route": sourceJob(t, "octo/missing", core.RunnerPool("linux"), 45),
	}
	for testName, job := range tests {
		t.Run(testName, func(t *testing.T) {
			assertJobRejected(t, source, job)
		})
	}
	if len(leader.acquired) != 0 || member.calls != 0 {
		t.Fatalf("invalid jobs reached routes: acquired=%#v bootstrap calls=%d", leader.acquired, member.calls)
	}
}

func TestSourcePropagatesRouteFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("route failed")
	leader := &leaderSessionFake{
		key: transport.SessionKey{Repository: "octo/repo", RunnerPool: "linux"}, err: wantErr,
	}
	member := &bootstrapClientFake{err: wantErr}
	source, err := githubsource.NewSource(
		[]githubsource.LeaderSession{leader},
		[]githubsource.MemberRoute{{
			Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), Client: member,
			Registration: transport.Registration{ID: 17, Name: "linux"},
		}},
	)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	job := sourceJob(t, "octo/repo", core.RunnerPool("linux"), 46)
	if err := source.Acquire(context.Background(), job); !errors.Is(err, wantErr) {
		t.Fatalf("Acquire() error = %v, want %v", err, wantErr)
	}
	if _, err := source.GenerateBootstrap(context.Background(), job, "runner"); !errors.Is(err, wantErr) {
		t.Fatalf("GenerateBootstrap() error = %v, want %v", err, wantErr)
	}
}

func TestNewSourceRejectsInvalidAndDuplicateLeaderRoutes(t *testing.T) {
	t.Parallel()

	key := transport.SessionKey{Repository: "octo/repo", RunnerPool: "linux"}
	validLeader := &leaderSessionFake{key: key}
	assertSourceConstructionFails(t, []githubsource.LeaderSession{nil}, nil)
	assertSourceConstructionFails(t, []githubsource.LeaderSession{&leaderSessionFake{}}, nil)
	assertSourceConstructionFails(
		t, []githubsource.LeaderSession{validLeader, &leaderSessionFake{key: key}}, nil,
	)
}

func TestNewSourceRejectsInvalidAndDuplicateMemberRoutes(t *testing.T) {
	t.Parallel()

	validMember := githubsource.MemberRoute{
		Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), Client: &bootstrapClientFake{},
		Registration: transport.Registration{ID: 17, Name: "linux"},
	}
	assertSourceConstructionFails(t, nil, []githubsource.MemberRoute{{}})
	assertSourceConstructionFails(t, nil, []githubsource.MemberRoute{{
		Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), Client: &bootstrapClientFake{},
	}})
	assertSourceConstructionFails(t, nil, []githubsource.MemberRoute{validMember, validMember})
}

func TestSourceRejectsEmptyBootstrapContent(t *testing.T) {
	t.Parallel()

	source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{{
		Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), Client: &bootstrapClientFake{},
		Registration: transport.Registration{ID: 17, Name: "linux"},
	}})
	if _, err := source.GenerateBootstrap(
		context.Background(), sourceJob(t, "octo/repo", core.RunnerPool("linux"), 47), "runner",
	); err == nil {
		t.Fatal("GenerateBootstrap() error = nil, want empty content failure")
	}
}

func newSourceWithMemberRoutes(t *testing.T, routes []githubsource.MemberRoute) *githubsource.Source {
	t.Helper()
	source, err := githubsource.NewSource(nil, routes)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	return source
}

func newRoutingFixture(t *testing.T) (*githubsource.Source, *leaderSessionFake, *bootstrapClientFake) {
	t.Helper()
	leader := &leaderSessionFake{key: transport.SessionKey{Repository: "octo/repo", RunnerPool: "linux"}}
	member := &bootstrapClientFake{content: []byte("opaque")}
	source, err := githubsource.NewSource(
		[]githubsource.LeaderSession{leader},
		[]githubsource.MemberRoute{{
			Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), Client: member,
			Registration: transport.Registration{ID: 17, Name: "linux"},
		}},
	)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	return source, leader, member
}

func assertJobRejected(t *testing.T, source *githubsource.Source, job core.Job) {
	t.Helper()
	if err := source.Acquire(context.Background(), job); err == nil {
		t.Fatal("Acquire() error = nil, want routing failure")
	}
	if _, err := source.GenerateBootstrap(context.Background(), job, "runner"); err == nil {
		t.Fatal("GenerateBootstrap() error = nil, want routing failure")
	}
}

func assertSourceConstructionFails(
	t *testing.T,
	leaders []githubsource.LeaderSession,
	members []githubsource.MemberRoute,
) {
	t.Helper()
	if _, err := githubsource.NewSource(leaders, members); err == nil {
		t.Fatal("NewSource() error = nil, want invalid route failure")
	}
}

func TestSourceReportsRunnerManagedDeliveryPolicy(t *testing.T) {
	t.Parallel()

	source, err := githubsource.NewSource(nil, nil)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	delivery, err := source.ReportResult(context.Background(), core.Job{
		Source: core.SourceReference{Adapter: githubsource.AdapterName},
	}, core.Result{Origin: core.ResultOriginRunner})
	if err != nil {
		t.Fatalf("report result: %v", err)
	}
	if delivery != core.ResultDeliveryRunnerManaged {
		t.Fatalf("delivery = %q, want runner managed", delivery)
	}
}

func sourceJob(t *testing.T, repository string, runnerPool core.RunnerPool, requestID int64) core.Job {
	t.Helper()
	sourceID, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: repository, RunnerPool: core.RunnerPool("linux"), RequestID: requestID,
	})
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	return core.Job{
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}, RunnerPool: runnerPool,
	}
}

type leaderSessionFake struct {
	key      transport.SessionKey
	acquired []int64
	err      error
}

func (session *leaderSessionFake) Key() transport.SessionKey { return session.key }

func (session *leaderSessionFake) AcquireRequest(_ context.Context, requestID int64) error {
	session.acquired = append(session.acquired, requestID)
	return session.err
}

type bootstrapClientFake struct {
	content      []byte
	err          error
	calls        int
	registration transport.Registration
	runnerName   string
}

func (client *bootstrapClientFake) GenerateBootstrap(
	_ context.Context,
	registration transport.Registration,
	runnerName string,
) ([]byte, error) {
	client.calls++
	client.registration = registration
	client.runnerName = runnerName
	return client.content, client.err
}
