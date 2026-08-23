package github_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestSourceIdentityRoundTripsDelimiterAndUnicodeContent(t *testing.T) {
	t.Parallel()

	want := githubsource.SourceIdentity{
		Repository: "octo|org/repo\x00snowman-☃",
		RunnerPool: core.RunnerPool("linux|x64/hosted"),
		RequestID:  42,
	}
	encoded, err := githubsource.EncodeSourceID(want)
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	got, err := githubsource.DecodeSourceID(encoded)
	if err != nil {
		t.Fatalf("decode source identity: %v", err)
	}
	if got != want {
		t.Fatalf("decoded identity = %#v, want %#v", got, want)
	}
}

func TestAggregateSourceIdentityRoundTripsWithoutInventingARequestID(t *testing.T) {
	t.Parallel()

	want := githubsource.SourceIdentity{
		Repository: "https://github.com/example/project",
		RunnerPool: core.RunnerPool("linux"),
		RecoveryID: uuid.MustParse("44f6cd03-2f2f-4da5-91f8-93a139efcc47"),
	}
	encoded, err := githubsource.EncodeSourceID(want)
	if err != nil {
		t.Fatalf("encode aggregate source identity: %v", err)
	}
	got, err := githubsource.DecodeSourceID(encoded)
	if err != nil {
		t.Fatalf("decode aggregate source identity: %v", err)
	}
	if got != want || got.RequestID != 0 {
		t.Fatalf("decoded identity = %#v, want %#v without a request ID", got, want)
	}
}

func TestAggregateDemandRequestCarriesTheOpaqueScope(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "message:17", DesiredJobs: 1, ObservedAt: time.Unix(17, 0)}
	request, err := githubsource.AggregateDemandRequest(demand, 1)
	if err != nil {
		t.Fatalf("AggregateDemandRequest(): %v", err)
	}
	identity, err := githubsource.DecodeSourceID(request.Source.ID)
	if err != nil {
		t.Fatalf("DecodeSourceID(): %v", err)
	}
	wantScope, err := githubsource.DemandScope(identity)
	if err != nil {
		t.Fatalf("DemandScope(): %v", err)
	}
	if request.Scope != wantScope || request.RunnerPool != demand.RunnerPool || request.EnqueuedAt != demand.ObservedAt {
		t.Fatalf("aggregate request = %#v, want scope %#v and demand fields", request, wantScope)
	}
}

func TestDemandScopeIsStableForPositiveAndAggregateIdentity(t *testing.T) {
	t.Parallel()

	repository := "https://github.com/example/project"
	runnerPool := core.RunnerPool("linux")
	positive, err := githubsource.DemandScope(githubsource.SourceIdentity{
		Repository: repository, RunnerPool: runnerPool, RequestID: 42,
	})
	if err != nil {
		t.Fatalf("positive demand scope: %v", err)
	}
	aggregate, err := githubsource.DemandScope(githubsource.SourceIdentity{
		Repository: repository, RunnerPool: runnerPool, RecoveryID: uuid.New(),
	})
	if err != nil {
		t.Fatalf("aggregate demand scope: %v", err)
	}
	if positive != aggregate || !positive.IsValid() {
		t.Fatalf("demand scopes = %#v and %#v, want one valid scope", positive, aggregate)
	}
}

func TestClassifyDemandIdentitySeparatesExactAndAggregateWork(t *testing.T) {
	t.Parallel()

	repository := "https://github.com/example/project"
	runnerPool := core.RunnerPool("linux")
	positive := encodeIdentity(t, githubsource.SourceIdentity{
		Repository: repository, RunnerPool: runnerPool, RequestID: 42,
	})
	aggregate := encodeIdentity(t, githubsource.SourceIdentity{
		Repository: repository, RunnerPool: runnerPool, RecoveryID: uuid.New(),
	})
	for _, testCase := range []struct {
		source        core.SourceReference
		wantMatch     bool
		wantAggregate bool
	}{
		{source: positive, wantMatch: true},
		{source: aggregate, wantMatch: true, wantAggregate: true},
		{source: core.SourceReference{Adapter: "other", ID: aggregate.ID}},
		{source: core.SourceReference{Adapter: githubsource.AdapterName, ID: "malformed"}},
	} {
		match, aggregateRecovery := githubsource.ClassifyDemandIdentity(testCase.source, repository, runnerPool)
		if match != testCase.wantMatch || aggregateRecovery != testCase.wantAggregate {
			t.Fatalf("ClassifyDemandIdentity(%#v) = (%t, %t), want (%t, %t)",
				testCase.source, match, aggregateRecovery, testCase.wantMatch, testCase.wantAggregate)
		}
	}
}

func TestAggregateDemandIdentityIsStableWithinOneObservation(t *testing.T) {
	t.Parallel()

	demand := githubsource.AssignedDemand{
		Repository: "https://github.com/example/project", RunnerPool: "linux",
		ObservationID: "message:73", DesiredJobs: 2,
	}
	first, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatalf("first aggregate source: %v", err)
	}
	repeated, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatalf("repeated aggregate source: %v", err)
	}
	second, err := githubsource.AggregateDemandSource(demand, 2)
	if err != nil {
		t.Fatalf("second aggregate source: %v", err)
	}
	if first != repeated || first == second {
		t.Fatalf("aggregate sources = %q, %q, %q; want stable first and distinct second", first.ID, repeated.ID, second.ID)
	}
}

func TestDemandIdentityHelpersRejectInvalidInput(t *testing.T) {
	t.Parallel()

	if _, err := githubsource.DemandScope(githubsource.SourceIdentity{}); err == nil {
		t.Fatal("DemandScope() accepted an incomplete identity")
	}
	for _, demand := range []githubsource.AssignedDemand{
		{},
		{Repository: "owner/repo", RunnerPool: "linux"},
	} {
		if _, err := githubsource.AggregateDemandSource(demand, 1); err == nil {
			t.Fatalf("AggregateDemandSource(%#v) accepted incomplete demand", demand)
		}
	}
	valid := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "message:1", ObservedAt: time.Unix(1, 0)}
	if _, err := githubsource.AggregateDemandSource(valid, 0); err == nil {
		t.Fatal("AggregateDemandSource() accepted a non-positive ordinal")
	}
	if _, err := githubsource.AggregateDemandRequest(githubsource.AssignedDemand{}, 1); err == nil {
		t.Fatal("AggregateDemandRequest() accepted invalid demand")
	}
	if _, err := githubsource.AggregateDemandRequest(valid, 0); err == nil {
		t.Fatal("AggregateDemandRequest() accepted a non-positive ordinal")
	}
}

func encodeIdentity(t *testing.T, identity githubsource.SourceIdentity) core.SourceReference {
	t.Helper()
	identifier, err := githubsource.EncodeSourceID(identity)
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	return core.SourceReference{Adapter: githubsource.AdapterName, ID: identifier}
}

func TestSourceIdentityEncodingDoesNotCollideAcrossRepositoryAndPoolBoundaries(t *testing.T) {
	t.Parallel()

	identities := []githubsource.SourceIdentity{
		{Repository: "owner/repo|linux", RunnerPool: core.RunnerPool("x64"), RequestID: 7},
		{Repository: "owner/repo", RunnerPool: core.RunnerPool("linux|x64"), RequestID: 7},
		{Repository: "owner/other", RunnerPool: core.RunnerPool("linux|x64"), RequestID: 7},
		{Repository: "owner/repo", RunnerPool: core.RunnerPool("linux|x64"), RequestID: 8},
	}
	encoded := make(map[core.SourceIdentifier]githubsource.SourceIdentity, len(identities))
	for _, identity := range identities {
		sourceID, err := githubsource.EncodeSourceID(identity)
		if err != nil {
			t.Fatalf("encode %#v: %v", identity, err)
		}
		if previous, exists := encoded[sourceID]; exists {
			t.Fatalf("identities %#v and %#v encoded as the same source ID", previous, identity)
		}
		encoded[sourceID] = identity
	}
}

func TestSourceIdentityRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	invalidIdentities := []githubsource.SourceIdentity{
		{RunnerPool: core.RunnerPool("linux"), RequestID: 1},
		{Repository: "owner/repo", RequestID: 1},
		{Repository: "owner/repo", RunnerPool: core.RunnerPool("linux")},
		{Repository: "owner/repo", RunnerPool: core.RunnerPool("linux"), RequestID: 1, RecoveryID: uuid.New()},
	}
	for _, identity := range invalidIdentities {
		if _, err := githubsource.EncodeSourceID(identity); err == nil {
			t.Fatalf("EncodeSourceID(%#v) error = nil, want validation failure", identity)
		}
	}
	invalidEncoded := []string{
		"",
		"not base64!",
		base64.RawURLEncoding.EncodeToString([]byte("%")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&request_id=not-number")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&request_id=1&request_id=2")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&recovery_id=not-a-uuid")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&request_id=0")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&request_id=1&extra=value")),
	}
	for _, encoded := range invalidEncoded {
		if _, err := githubsource.DecodeSourceID(core.SourceIdentifier(encoded)); err == nil {
			t.Fatalf("DecodeSourceID(%q) error = nil, want validation failure", encoded)
		}
	}
}
