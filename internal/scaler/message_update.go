package scaler

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	// ErrNotLeader reports that a message belongs to an obsolete controller epoch.
	// It is terminal for that message-session generation and must not be retried or acknowledged.
	ErrNotLeader            = errors.New("message update controller epoch is obsolete")
	ErrInvalidMessageUpdate = errors.New("invalid scale-set message update")
)

// RunnerMessageIdentity identifies a started or completed runner without
// inventing a request identity when GitHub supplies only the runner name.
type RunnerMessageIdentity struct {
	RunnerIdentity
	GitHubRunnerID int64
}

// MessageUpdate contains every durable effect carried by one GitHub scale-set
// message. Stores validate ControllerEpoch before applying any mutation and
// commit the complete update atomically.
type MessageUpdate struct {
	AppID           int64
	Repository      string
	ScaleSet        string
	ControllerEpoch int64
	AssignedJobs    int
	ActiveCapacity  int
	ObservedAt      time.Time
	Available       []JobDemand
	Started         []RunnerMessageIdentity
	Completed       []RunnerMessageIdentity
}

type MessageUpdateResult struct {
	AssignedObligations AssignedObligationDecision
	QueueSnapshot       CapacityObligationQueueSnapshot
	ControllerEpoch     int64
}

// CapacityObligationQueueSnapshot describes one repository and scale set's
// durable queue immediately after a message update commits its effects.
type CapacityObligationQueueSnapshot struct {
	JobAvailable     int
	AssignedCount    int
	NewlyQueued      int
	Canceled         int
	OldestRunnableAt *time.Time
}

type MessageUpdateStore interface {
	ApplyMessageUpdate(ctx context.Context, update MessageUpdate) (MessageUpdateResult, error)
}

func ValidMessageUpdate(update MessageUpdate) bool {
	return validMessageUpdateMetadata(update) && validMessageDemands(update) &&
		validMessageIdentities(update.Started) && validMessageIdentities(update.Completed)
}

func validMessageUpdateMetadata(update MessageUpdate) bool {
	return update.AppID > 0 && strings.TrimSpace(update.Repository) != "" && strings.TrimSpace(update.ScaleSet) != "" &&
		update.ControllerEpoch > 0 && update.AssignedJobs >= 0 && update.ActiveCapacity >= 0 && !update.ObservedAt.IsZero()
}

func validMessageDemands(update MessageUpdate) bool {
	for _, demand := range update.Available {
		if demand.RequestID <= 0 || demand.QueueTime.IsZero() || demand.Repository != update.Repository {
			return false
		}
	}
	return true
}

func validMessageIdentities(identities []RunnerMessageIdentity) bool {
	for _, identity := range identities {
		if identity.Validate() != nil {
			return false
		}
	}
	return true
}
