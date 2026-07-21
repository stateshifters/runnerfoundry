package cluster

import "github.com/meza/github-scaler/internal/githubapp"

var (
	_ AssignmentID             = Assignment{}.ID
	_ NodeID                   = Assignment{}.NodeID
	_ githubapp.InstallationID = Assignment{}.InstallationID
	_ AssignmentID             = AssignmentTransitionError{}.AssignmentID
)
