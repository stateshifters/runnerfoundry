package runtime

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

const runnerNamePrefix = "runnerfoundry-"

func runnerName(jobID core.JobID) core.RunnerName {
	return core.RunnerName(runnerNamePrefix + jobID.String())
}

func jobIDFromRunnerName(name core.RunnerName) (core.JobID, bool, error) {
	identifier, canonical := strings.CutPrefix(string(name), runnerNamePrefix)
	if !canonical {
		return uuid.Nil, false, nil
	}
	jobID, err := uuid.Parse(identifier)
	if err != nil || jobID == uuid.Nil {
		return uuid.Nil, true, fmt.Errorf("parse canonical runner name %q: %w", name, core.ErrInvalidJobIdentity)
	}
	return jobID, true, nil
}

func isRunnerFoundryName(name core.RunnerName) bool {
	_, canonical, err := jobIDFromRunnerName(name)
	return canonical && err == nil
}
