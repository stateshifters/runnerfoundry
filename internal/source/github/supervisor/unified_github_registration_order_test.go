package supervisor

import (
	"reflect"
	"testing"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestCanonicalRegistrationsOrdersEveryMaterialRegistrationFact(t *testing.T) {
	records := []githubsource.Registration{
		{RepositoryURL: "second", RunnerPool: "linux", InstallationID: 1, RunnerGroupID: 1, ScaleSetID: 1},
		{RepositoryURL: "first", RunnerPool: "windows", InstallationID: 1, RunnerGroupID: 1, ScaleSetID: 1},
		{RepositoryURL: "first", RunnerPool: "linux", InstallationID: 2, RunnerGroupID: 1, ScaleSetID: 1},
		{RepositoryURL: "first", RunnerPool: "linux", InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 1},
		{RepositoryURL: "first", RunnerPool: "linux", InstallationID: 1, RunnerGroupID: 1, ScaleSetID: 2},
		{RepositoryURL: "first", RunnerPool: "linux", InstallationID: 1, RunnerGroupID: 1, ScaleSetID: 1},
	}
	canonical := canonicalRegistrations(records)
	want := []githubsource.Registration{records[5], records[4], records[3], records[2], records[1], records[0]}
	if !reflect.DeepEqual(canonical, want) {
		t.Fatalf("canonical registrations = %#v, want %#v", canonical, want)
	}
}
