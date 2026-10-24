package core

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const attemptRunnerNamePrefix = "runnerfoundry-v2-"
const runnerIdentityTokenLength = 22

// ErrInvalidRunnerName identifies malformed data inside RunnerFoundry's owned
// runner-name namespace. Names outside that namespace are not errors.
var ErrInvalidRunnerName = errors.New("invalid RunnerFoundry runner name")

type RunnerIdentity struct {
	JobID     JobID
	AttemptID AttemptID
}

func NewAttemptID() AttemptID {
	return AttemptID(uuid.New())
}

func (attemptID AttemptID) IsValid() bool {
	return uuid.UUID(attemptID) != uuid.Nil
}

// AttemptRunnerName encodes non-zero durable job and attempt identities without
// exceeding the practical runner-name limits of supported providers. Invalid
// identities return an empty name outside RunnerFoundry's owned namespace.
func AttemptRunnerName(jobID JobID, attemptID AttemptID) RunnerName {
	if jobID == uuid.Nil || !attemptID.IsValid() {
		return ""
	}
	jobToken := base64.RawURLEncoding.EncodeToString(jobID[:])
	attemptUUID := uuid.UUID(attemptID)
	attemptToken := base64.RawURLEncoding.EncodeToString(attemptUUID[:])
	return RunnerName(attemptRunnerNamePrefix + jobToken + "-" + attemptToken)
}

// ParseRunnerName decodes a RunnerFoundry v2 runner name. The owned result is
// false for names outside the v2 namespace. Malformed names within that
// namespace return ErrInvalidRunnerName with owned set to true.
func ParseRunnerName(name RunnerName) (identity RunnerIdentity, owned bool, err error) {
	encoded, owned := strings.CutPrefix(string(name), attemptRunnerNamePrefix)
	if !owned {
		return RunnerIdentity{}, false, nil
	}
	if len(encoded) != runnerIdentityTokenLength+1+runnerIdentityTokenLength ||
		encoded[runnerIdentityTokenLength] != '-' {
		return RunnerIdentity{}, true, invalidRunnerName(name)
	}
	jobID, err := decodeRunnerIdentityToken(encoded[:runnerIdentityTokenLength])
	if err != nil {
		return RunnerIdentity{}, true, invalidRunnerName(name)
	}
	attemptUUID, err := decodeRunnerIdentityToken(encoded[runnerIdentityTokenLength+1:])
	if err != nil {
		return RunnerIdentity{}, true, invalidRunnerName(name)
	}
	return RunnerIdentity{JobID: jobID, AttemptID: AttemptID(attemptUUID)}, true, nil
}

func IsRunnerFoundryRunnerName(name RunnerName) bool {
	_, owned, err := ParseRunnerName(name)
	return owned && err == nil
}

func decodeRunnerIdentityToken(encoded string) (uuid.UUID, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != 16 {
		return uuid.Nil, ErrInvalidRunnerName
	}
	identity, err := uuid.FromBytes(decoded)
	if err != nil || identity == uuid.Nil {
		return uuid.Nil, ErrInvalidRunnerName
	}
	return identity, nil
}

func invalidRunnerName(name RunnerName) error {
	return fmt.Errorf("parse RunnerFoundry runner name %q: %w", name, ErrInvalidRunnerName)
}
