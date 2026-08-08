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

func TestSourceRoutesBootstrapByRepositoryAndRunnerPool(t *testing.T) {
	t.Parallel()

	first := &bootstrapClientFake{content: []byte("wrong")}
	second := &bootstrapClientFake{content: []byte("opaque-jit")}
	source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{
		memberRoute("octo/first", first),
		memberRoute("octo/second", second),
	})
	bootstrap, err := source.GenerateBootstrap(
		context.Background(), sourceJob(t, "octo/second", core.RunnerPool("linux"), 43), "runner-43",
	)
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

func TestSourceRoutesAggregateDemandThroughTheOrdinaryBootstrapPath(t *testing.T) {
	t.Parallel()

	client := &bootstrapClientFake{content: []byte("opaque-jit")}
	source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{memberRoute("octo/repo", client)})
	reference, err := githubsource.AggregateDemandSource(githubsource.AssignedDemand{
		Repository: "octo/repo", RunnerPool: "linux", ObservationID: "message:17", DesiredJobs: 1,
	}, 1)
	if err != nil {
		t.Fatalf("identify aggregate demand: %v", err)
	}
	job := core.Job{ID: core.JobID{1}, Source: reference, RunnerPool: "linux"}
	bootstrap, err := source.GenerateBootstrap(context.Background(), job, "runnerfoundry-job")
	if err != nil {
		t.Fatalf("generate aggregate-demand bootstrap: %v", err)
	}
	if client.calls != 1 || client.runnerName != "runnerfoundry-job" ||
		!reflect.DeepEqual(bootstrap, execution.NewBootstrap([]byte("opaque-jit"))) {
		t.Fatalf("aggregate bootstrap = %#v, calls %d, runner %q", bootstrap, client.calls, client.runnerName)
	}
}

func TestSourceRejectsInvalidRoutes(t *testing.T) {
	t.Parallel()

	valid := memberRoute("octo/repo", &bootstrapClientFake{})
	tests := map[string][]githubsource.MemberRoute{
		"missing repository": {{RunnerPool: valid.RunnerPool, Client: valid.Client, Registration: valid.Registration}},
		"missing pool":       {{Repository: valid.Repository, Client: valid.Client, Registration: valid.Registration}},
		"missing client":     {{Repository: valid.Repository, RunnerPool: valid.RunnerPool, Registration: valid.Registration}},
		"invalid registration": {{
			Repository: valid.Repository, RunnerPool: valid.RunnerPool, Client: valid.Client,
			Registration: transport.Registration{ID: 0, Name: "linux"},
		}},
		"mismatched registration": {{
			Repository: valid.Repository, RunnerPool: valid.RunnerPool, Client: valid.Client,
			Registration: transport.Registration{ID: 17, Name: "windows"},
		}},
		"duplicate": {valid, valid},
	}
	for testName, routes := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			if _, err := githubsource.NewSource(routes); err == nil {
				t.Fatal("NewSource() error = nil, want invalid route failure")
			}
		})
	}
}

func TestSourceRejectsInvalidJobsAndMissingRoutes(t *testing.T) {
	t.Parallel()

	client := &bootstrapClientFake{content: []byte("opaque")}
	source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{memberRoute("octo/repo", client)})
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
			t.Parallel()
			if _, err := source.GenerateBootstrap(context.Background(), job, "runner"); err == nil {
				t.Fatal("GenerateBootstrap() error = nil, want routing failure")
			}
		})
	}
	if client.calls != 0 {
		t.Fatalf("invalid jobs reached bootstrap client %d times", client.calls)
	}
}

func TestSourcePropagatesBootstrapFailures(t *testing.T) {
	t.Parallel()

	tests := map[string]*bootstrapClientFake{
		"client error":  {err: errors.New("bootstrap failed")},
		"empty content": {},
	}
	for testName, client := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{memberRoute("octo/repo", client)})
			if _, err := source.GenerateBootstrap(
				context.Background(), sourceJob(t, "octo/repo", core.RunnerPool("linux"), 46), "runner",
			); err == nil {
				t.Fatal("GenerateBootstrap() error = nil, want bootstrap failure")
			}
		})
	}
}

func TestSourceReplaceRoutesIsAtomic(t *testing.T) {
	t.Parallel()

	oldClient := &bootstrapClientFake{content: []byte("old")}
	newClient := &bootstrapClientFake{content: []byte("new")}
	source := newSourceWithMemberRoutes(t, []githubsource.MemberRoute{memberRoute("octo/repo", oldClient)})
	if err := source.ReplaceRoutes([]githubsource.MemberRoute{memberRoute("octo/new", newClient)}); err != nil {
		t.Fatalf("replace routes: %v", err)
	}
	if _, err := source.GenerateBootstrap(
		context.Background(), sourceJob(t, "octo/new", core.RunnerPool("linux"), 47), "runner",
	); err != nil {
		t.Fatalf("use replacement route: %v", err)
	}
	if err := source.ReplaceRoutes([]githubsource.MemberRoute{{Repository: "invalid"}}); err == nil {
		t.Fatal("ReplaceRoutes() error = nil, want invalid replacement")
	}
	if _, err := source.GenerateBootstrap(
		context.Background(), sourceJob(t, "octo/new", core.RunnerPool("linux"), 48), "runner",
	); err != nil {
		t.Fatalf("use preserved route: %v", err)
	}
	if oldClient.calls != 0 || newClient.calls != 2 {
		t.Fatalf("bootstrap calls: old=%d new=%d", oldClient.calls, newClient.calls)
	}
}

func newSourceWithMemberRoutes(t *testing.T, routes []githubsource.MemberRoute) *githubsource.Source {
	t.Helper()
	source, err := githubsource.NewSource(routes)
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	return source
}

func memberRoute(repository string, client githubsource.BootstrapClient) githubsource.MemberRoute {
	return githubsource.MemberRoute{
		Repository: repository, RunnerPool: core.RunnerPool("linux"), Client: client,
		Registration: transport.Registration{ID: 17, Name: "linux"},
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
	return core.Job{Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}, RunnerPool: runnerPool}
}

type bootstrapClientFake struct {
	content      []byte
	err          error
	calls        int
	registration transport.Registration
	runnerName   core.RunnerName
}

func (client *bootstrapClientFake) GenerateBootstrap(
	_ context.Context,
	registration transport.Registration,
	runnerName core.RunnerName,
) ([]byte, error) {
	client.calls++
	client.registration = registration
	client.runnerName = runnerName
	return client.content, client.err
}
