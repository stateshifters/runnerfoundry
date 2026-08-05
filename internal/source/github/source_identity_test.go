package github_test

import (
	"encoding/base64"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
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

func TestSourceIdentityEncodingDoesNotCollideAcrossRepositoryAndPoolBoundaries(t *testing.T) {
	t.Parallel()

	identities := []githubsource.SourceIdentity{
		{Repository: "owner/repo|linux", RunnerPool: core.RunnerPool("x64"), RequestID: 7},
		{Repository: "owner/repo", RunnerPool: core.RunnerPool("linux|x64"), RequestID: 7},
		{Repository: "owner/other", RunnerPool: core.RunnerPool("linux|x64"), RequestID: 7},
		{Repository: "owner/repo", RunnerPool: core.RunnerPool("linux|x64"), RequestID: 8},
	}
	encoded := make(map[string]githubsource.SourceIdentity, len(identities))
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
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&request_id=0")),
		base64.RawURLEncoding.EncodeToString([]byte("repository=repo&runner_pool=linux&request_id=1&extra=value")),
	}
	for _, encoded := range invalidEncoded {
		if _, err := githubsource.DecodeSourceID(encoded); err == nil {
			t.Fatalf("DecodeSourceID(%q) error = nil, want validation failure", encoded)
		}
	}
}
