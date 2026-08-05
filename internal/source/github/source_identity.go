package github

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/meza/RunnerFoundry/internal/core"
)

// SourceIdentity is the GitHub-owned identity encoded in a core source
// reference. Repository and RunnerPool select the scale-set session, while
// RequestID is the numeric identifier accepted by GitHub's AcquireJobs API.
type SourceIdentity struct {
	Repository string
	RunnerPool core.RunnerPool
	RequestID  int64
}

// EncodeSourceID returns an opaque, deterministic core source ID. The encoded
// query encoding is base64url without padding, so arbitrary repository and pool
// names cannot create delimiter collisions. Invalid identities are rejected.
func EncodeSourceID(identity SourceIdentity) (string, error) {
	if err := validateSourceIdentity(identity); err != nil {
		return "", err
	}
	values := url.Values{
		"repository":  []string{identity.Repository},
		"runner_pool": []string{string(identity.RunnerPool)},
		"request_id":  []string{strconv.FormatInt(identity.RequestID, 10)},
	}
	return base64.RawURLEncoding.EncodeToString([]byte(values.Encode())), nil
}

// DecodeSourceID decodes and validates an opaque GitHub source ID. Callers
// must continue to verify that the decoded pool matches the core job before
// selecting a repository and scale-set session.
func DecodeSourceID(sourceID string) (SourceIdentity, error) {
	encodedValues, err := base64.RawURLEncoding.DecodeString(sourceID)
	if err != nil {
		return SourceIdentity{}, fmt.Errorf("decode GitHub source ID: %w", err)
	}
	values, err := url.ParseQuery(string(encodedValues))
	if err != nil {
		return SourceIdentity{}, fmt.Errorf("decode GitHub source identity: %w", err)
	}
	if len(values) != 3 || len(values["repository"]) != 1 ||
		len(values["runner_pool"]) != 1 || len(values["request_id"]) != 1 {
		return SourceIdentity{}, errors.New("GitHub source identity has invalid fields")
	}
	requestID, err := strconv.ParseInt(values.Get("request_id"), 10, 64)
	if err != nil {
		return SourceIdentity{}, fmt.Errorf("decode GitHub runner request ID: %w", err)
	}
	identity := SourceIdentity{
		Repository: values.Get("repository"),
		RunnerPool: core.RunnerPool(values.Get("runner_pool")),
		RequestID:  requestID,
	}
	if err := validateSourceIdentity(identity); err != nil {
		return SourceIdentity{}, err
	}
	return identity, nil
}

func validateSourceIdentity(identity SourceIdentity) error {
	if identity.Repository == "" {
		return errors.New("GitHub source repository is required")
	}
	if identity.RunnerPool == "" {
		return errors.New("GitHub source Runner Pool is required")
	}
	if identity.RequestID <= 0 {
		return errors.New("GitHub runner request ID must be positive")
	}
	return nil
}
