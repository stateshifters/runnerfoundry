package core

import "time"

type ObservationResult struct{ Outcome ObservationOutcome }

type ObservationOutcome string

const (
	ObservationApplied        ObservationOutcome = "applied"
	ObservationAlreadyApplied ObservationOutcome = "already_applied"
	ObservationUnmatched      ObservationOutcome = "unmatched"
)

type JobRequest struct {
	Source     SourceReference
	Scope      DemandScope
	RunnerPool RunnerPool
	EnqueuedAt time.Time
}

type Job struct {
	ID         JobID
	Source     SourceReference
	RunnerPool RunnerPool
	EnqueuedAt time.Time
}

type ProcessingJob struct {
	Job                 Job
	RunnerName          RunnerName
	MemberID            MemberID
	ClaimedAt           time.Time
	InventoryEndpointID InventoryEndpointID
	RunnerAccepted      bool
	SourceTerminal      bool
	ProvisioningFailure ProvisioningFailureState
}

type ProvisioningFailureState string

const (
	ProvisioningFailureOpen      ProvisioningFailureState = ""
	ProvisioningFailureReporting ProvisioningFailureState = "reporting"
	ProvisioningFailureReported  ProvisioningFailureState = "reported"
)

type ProcessingClaim struct {
	MemberID            MemberID
	AttemptID           AttemptID
	ClaimedAt           time.Time
	InventoryEndpointID InventoryEndpointID
}

type EnqueueResult struct {
	Job     Job
	Outcome QueueOutcome
}

type QueueOutcome string

const (
	QueueQueued         QueueOutcome = "queued"
	QueueAlreadyQueued  QueueOutcome = "already_queued"
	QueueProcessing     QueueOutcome = "processing"
	QueueSourceTerminal QueueOutcome = "source_terminal"
)

type ClaimResult struct {
	Processing ProcessingJob
	Outcome    ClaimOutcome
}

type ClaimOutcome string

const (
	ClaimClaimed ClaimOutcome = "claimed"
	ClaimEmpty   ClaimOutcome = "empty"
)

type RetryResult struct{ Outcome RetryOutcome }

type RetryOutcome string

const (
	RetryRequeued                   RetryOutcome = "requeued"
	RetryMissing                    RetryOutcome = "missing"
	RetryRunnerAccepted             RetryOutcome = "runner_accepted"
	RetrySourceTerminal             RetryOutcome = "source_terminal"
	RetryClaimChanged               RetryOutcome = "claim_changed"
	RetryCleanupPending             RetryOutcome = "cleanup_pending"
	RetryFailureReportingPending    RetryOutcome = "failure_reporting_pending"
	RetryFailureFinalizationPending RetryOutcome = "failure_finalization_pending"
)

type TerminalRemovalResult struct{ Outcome TerminalRemovalOutcome }

type TerminalRemovalOutcome string

const (
	TerminalRemoved     TerminalRemovalOutcome = "removed"
	TerminalMissing     TerminalRemovalOutcome = "missing"
	TerminalNotTerminal TerminalRemovalOutcome = "not_terminal"
)

// RunnerPoolCapacity publishes one member's currently free local slots. A
// non-positive capacity withdraws that member's capacity for the Runner Pool.
type RunnerPoolCapacity struct {
	RunnerPool RunnerPool
	Capacity   int
}
