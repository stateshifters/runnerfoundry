package scaler

import (
	"math"
	"strings"
	"testing"
)

func TestRunnerNamePreservesExistingShortName(t *testing.T) {
	runnerName := runnerName("github-com-meza-repo-a-Linux", 1)

	if runnerName != "github-com-meza-repo-a-Linux-1" {
		t.Fatalf("unexpected runner name %q", runnerName)
	}
}

func TestRunnerNameCompactsLongRepositoryQualifiedName(t *testing.T) {
	baseName := "github-com-meza-mezas-conventional-changelog-meza-ubuntu-latest"

	runnerName := runnerName(baseName, 0)

	expectedName := "github-com-meza-mezas-conventional-changelog-ee540047238cb46a-0"
	if runnerName != expectedName {
		t.Fatalf("expected %q, got %q", expectedName, runnerName)
	}
}

func TestRunnerNameNormalizesUnsafeCharactersWithoutLosingIdentity(t *testing.T) {
	runnerName := runnerName("répo/生产", 42)

	expectedName := "r-po-d1bdf36cc60e9872-42"
	if runnerName != expectedName {
		t.Fatalf("expected %q, got %q", expectedName, runnerName)
	}
}

func TestRunnerNameRetainsMaximumNumericIdentifierWithinLimit(t *testing.T) {
	runnerName := runnerName(strings.Repeat("long-name-", 10), math.MaxInt64)

	if len(runnerName) > maximumRunnerNameLength {
		t.Fatalf("runner name has %d characters: %q", len(runnerName), runnerName)
	}
	if !strings.HasSuffix(runnerName, "-9223372036854775807") {
		t.Fatalf("runner name lost its identifier: %q", runnerName)
	}
}

func TestRunnerNameDistinguishesLongNamesWithSharedPrefix(t *testing.T) {
	sharedPrefix := strings.Repeat("shared-prefix-", 5)
	firstName := runnerName(sharedPrefix+"alpha", 7)
	secondName := runnerName(sharedPrefix+"bravo", 7)

	if firstName == secondName {
		t.Fatalf("expected distinct runner names, got %q", firstName)
	}
}

func TestRunnerNameUsesFallbackWhenBaseHasNoSafeCharacters(t *testing.T) {
	runnerName := runnerName("生产", 9)

	if runnerName != "runner-76ab7d3b41f2326d-9" {
		t.Fatalf("unexpected fallback runner name %q", runnerName)
	}
}
