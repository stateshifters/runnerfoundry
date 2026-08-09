package runtime

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

const runnerNamePrefix = "runnerfoundry-"
const attemptRunnerNamePrefix = runnerNamePrefix + "v2-"

func legacyRunnerName(jobID core.JobID) core.RunnerName {
	return core.RunnerName(runnerNamePrefix + jobID.String())
}

func jobIDFromRunnerName(name core.RunnerName) (core.JobID, bool, error) {
	if strings.HasPrefix(string(name), attemptRunnerNamePrefix) {
		return jobIDFromAttemptRunnerName(name)
	}
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

func jobIDFromAttemptRunnerName(name core.RunnerName) (core.JobID, bool, error) {
	const tokenLength = 22
	encoded := strings.TrimPrefix(string(name), attemptRunnerNamePrefix)
	if len(encoded) != tokenLength+1+tokenLength || encoded[tokenLength] != '-' {
		return uuid.Nil, true, fmt.Errorf("parse canonical runner name %q: %w", name, core.ErrInvalidJobIdentity)
	}
	jobBytes, err := base64.RawURLEncoding.DecodeString(encoded[:tokenLength])
	if err != nil || len(jobBytes) != 16 {
		return uuid.Nil, true, fmt.Errorf("parse canonical runner name %q: %w", name, core.ErrInvalidJobIdentity)
	}
	jobID, err := uuid.FromBytes(jobBytes)
	if err != nil || jobID == uuid.Nil {
		return uuid.Nil, true, fmt.Errorf("parse canonical runner name %q: %w", name, core.ErrInvalidJobIdentity)
	}
	if _, err = base64.RawURLEncoding.DecodeString(encoded[tokenLength+1:]); err != nil {
		return uuid.Nil, true, fmt.Errorf("parse canonical runner name %q: %w", name, core.ErrInvalidJobIdentity)
	}
	return jobID, true, nil
}

func processingRunnerName(processing core.ProcessingJob) core.RunnerName {
	if processing.RunnerName != "" {
		return processing.RunnerName
	}
	return legacyRunnerName(processing.Job.ID)
}

func isRunnerFoundryName(name core.RunnerName) bool {
	_, canonical, err := jobIDFromRunnerName(name)
	return canonical && err == nil
}
