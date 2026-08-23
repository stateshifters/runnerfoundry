package github_test

import (
	"testing"
	"time"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestAssignedDemandValidationRejectsIncompleteAndImpossibleObservations(t *testing.T) {
	t.Parallel()

	valid := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "message:1", ObservedAt: time.Unix(1, 0)}
	invalid := []githubsource.AssignedDemand{
		{},
		{RunnerPool: valid.RunnerPool, ObservationID: valid.ObservationID, ObservedAt: valid.ObservedAt},
		{Repository: valid.Repository, ObservationID: valid.ObservationID, ObservedAt: valid.ObservedAt},
		{Repository: valid.Repository, RunnerPool: valid.RunnerPool, ObservedAt: valid.ObservedAt},
		{Repository: valid.Repository, RunnerPool: valid.RunnerPool, ObservationID: valid.ObservationID},
		{Repository: valid.Repository, RunnerPool: valid.RunnerPool, ObservationID: valid.ObservationID,
			DesiredJobs: -1, ObservedAt: valid.ObservedAt},
	}
	for _, demand := range invalid {
		if err := demand.Validate(); err == nil {
			t.Fatalf("AssignedDemand.Validate(%#v) error = nil", demand)
		}
	}
}
