package github

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

var aggregateDemandNamespace = uuid.MustParse("07f30a90-34fe-5df8-a86c-b3c7d2a50904")

// SourceIdentity is the GitHub-owned identity encoded in a core source
// reference. Repository and RunnerPool select the scale-set session, while
// RequestID is the numeric identifier accepted by GitHub's AcquireJobs API.
type SourceIdentity struct {
	Repository string
	RunnerPool core.RunnerPool
	RequestID  int64
	RecoveryID uuid.UUID
}

// EncodeSourceID returns an opaque, deterministic core source ID. The encoded
// query encoding is base64url without padding, so arbitrary repository and pool
// names cannot create delimiter collisions. Invalid identities are rejected.
func EncodeSourceID(identity SourceIdentity) (core.SourceIdentifier, error) {
	if err := validateSourceIdentity(identity); err != nil {
		return "", err
	}
	return encodeSourceIdentity(identity), nil
}

func encodeSourceIdentity(identity SourceIdentity) core.SourceIdentifier {
	values := url.Values{
		"repository":  []string{identity.Repository},
		"runner_pool": []string{string(identity.RunnerPool)},
	}
	if identity.RequestID > 0 {
		values.Set("request_id", strconv.FormatInt(identity.RequestID, 10))
	} else {
		values.Set("recovery_id", identity.RecoveryID.String())
	}
	return core.SourceIdentifier(base64.RawURLEncoding.EncodeToString([]byte(values.Encode())))
}

// DemandScope returns the stable repository and Runner Pool key shared by
// positive-request and aggregate-recovery demand.
func DemandScope(identity SourceIdentity) (core.DemandScope, error) {
	if err := validateSourceIdentity(identity); err != nil {
		return core.DemandScope{}, err
	}
	return scopeForIdentity(identity), nil
}

func scopeForIdentity(identity SourceIdentity) core.DemandScope {
	values := url.Values{
		"repository":  []string{identity.Repository},
		"runner_pool": []string{string(identity.RunnerPool)},
	}
	return core.DemandScope{Adapter: AdapterName,
		ID: core.SourceIdentifier(base64.RawURLEncoding.EncodeToString([]byte(values.Encode())))}
}

// ClassifyDemandIdentity reports whether a durable source reference belongs
// to one repository and Runner Pool, and whether it represents aggregate
// recovery rather than an exact GitHub runner request.
func ClassifyDemandIdentity(
	source core.SourceReference,
	repository string,
	runnerPool core.RunnerPool,
) (matches bool, aggregateRecovery bool) {
	if source.Adapter != AdapterName {
		return false, false
	}
	identity, err := DecodeSourceID(source.ID)
	if err != nil || identity.Repository != repository || identity.RunnerPool != runnerPool {
		return false, false
	}
	return true, identity.RecoveryID != uuid.Nil
}

// AggregateDemandSource deterministically identifies one anonymous unit in an
// authoritative GitHub assigned-demand observation. Redelivery therefore
// cannot recreate a unit already protected by its source tombstone.
func AggregateDemandSource(demand AssignedDemand, ordinal int) (core.SourceReference, error) {
	if demand.Repository == "" || demand.RunnerPool == "" || demand.ObservationID == "" || ordinal <= 0 {
		return core.SourceReference{}, errors.New("complete GitHub demand and positive ordinal are required")
	}
	identity := aggregateDemandIdentity(demand, ordinal)
	return core.SourceReference{Adapter: AdapterName, ID: encodeSourceIdentity(identity)}, nil
}

// AggregateDemandRequest creates an ordinary queued-job request while keeping
// the GitHub-owned identity and serialization scope opaque to durable state.
func AggregateDemandRequest(demand AssignedDemand, ordinal int) (core.JobRequest, error) {
	if err := demand.Validate(); err != nil {
		return core.JobRequest{}, err
	}
	if ordinal <= 0 {
		return core.JobRequest{}, errors.New("positive aggregate demand ordinal is required")
	}
	identity := aggregateDemandIdentity(demand, ordinal)
	source := core.SourceReference{Adapter: AdapterName, ID: encodeSourceIdentity(identity)}
	return core.JobRequest{Source: source, Scope: scopeForIdentity(identity),
		RunnerPool: demand.RunnerPool, EnqueuedAt: demand.ObservedAt}, nil
}

func aggregateDemandIdentity(demand AssignedDemand, ordinal int) SourceIdentity {
	identityMaterial := fmt.Sprintf("%s\x00%s\x00%s\x00%d",
		demand.Repository, demand.RunnerPool, demand.ObservationID, ordinal)
	return SourceIdentity{Repository: demand.Repository, RunnerPool: demand.RunnerPool,
		RecoveryID: uuid.NewSHA1(aggregateDemandNamespace, []byte(identityMaterial))}
}

// DecodeSourceID decodes and validates an opaque GitHub source ID. Callers
// must continue to verify that the decoded pool matches the core job before
// selecting a repository and scale-set session.
func DecodeSourceID(sourceID core.SourceIdentifier) (SourceIdentity, error) {
	values, err := decodeSourceIdentityValues(sourceID)
	if err != nil {
		return SourceIdentity{}, err
	}
	requestID, recoveryID, err := decodeWorkIdentity(values)
	if err != nil {
		return SourceIdentity{}, err
	}
	identity := SourceIdentity{Repository: values.Get("repository"),
		RunnerPool: core.RunnerPool(values.Get("runner_pool")), RequestID: requestID, RecoveryID: recoveryID}
	if err := validateSourceIdentity(identity); err != nil {
		return SourceIdentity{}, err
	}
	return identity, nil
}

func decodeSourceIdentityValues(sourceID core.SourceIdentifier) (url.Values, error) {
	encodedValues, err := base64.RawURLEncoding.DecodeString(string(sourceID))
	if err != nil {
		return nil, fmt.Errorf("decode GitHub source ID: %w", err)
	}
	values, err := url.ParseQuery(string(encodedValues))
	if err != nil {
		return nil, fmt.Errorf("decode GitHub source identity: %w", err)
	}
	if len(values) != 3 || len(values["repository"]) != 1 || len(values["runner_pool"]) != 1 {
		return nil, errors.New("GitHub source identity has invalid fields")
	}
	return values, nil
}

func decodeWorkIdentity(values url.Values) (int64, uuid.UUID, error) {
	requestValues, hasRequest := values["request_id"]
	recoveryValues, hasRecovery := values["recovery_id"]
	if hasRequest == hasRecovery || (hasRequest && len(requestValues) != 1) || (hasRecovery && len(recoveryValues) != 1) {
		return 0, uuid.Nil, errors.New("GitHub source identity has invalid work identity")
	}
	if hasRequest {
		requestID, err := strconv.ParseInt(requestValues[0], 10, 64)
		if err != nil {
			return 0, uuid.Nil, fmt.Errorf("decode GitHub runner request ID: %w", err)
		}
		return requestID, uuid.Nil, nil
	}
	recoveryID, err := uuid.Parse(recoveryValues[0])
	if err != nil {
		return 0, uuid.Nil, fmt.Errorf("decode GitHub aggregate recovery ID: %w", err)
	}
	return 0, recoveryID, nil
}

func validateSourceIdentity(identity SourceIdentity) error {
	if identity.Repository == "" {
		return errors.New("GitHub source repository is required")
	}
	if identity.RunnerPool == "" {
		return errors.New("GitHub source Runner Pool is required")
	}
	requestIdentity := identity.RequestID > 0 && identity.RecoveryID == uuid.Nil
	aggregateIdentity := identity.RequestID == 0 && identity.RecoveryID != uuid.Nil
	if !requestIdentity && !aggregateIdentity {
		return errors.New("GitHub source identity requires one positive request ID or aggregate recovery ID")
	}
	return nil
}
