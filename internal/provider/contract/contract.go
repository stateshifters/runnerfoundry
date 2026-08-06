// Package contract defines provider-neutral ownership, bootstrap, and lifecycle
// values shared by execution backends. New resources use Runner Pool markers;
// legacy scale-set markers remain read-only compatibility inputs.
package contract

const (
	JITConfigEnvironment    = "ACTIONS_RUNNER_INPUT_JITCONFIG"
	JITOEMStringPrefix      = "runnerfoundry.jit="
	OwnershipRunnerPool     = "runnerfoundry.runner-pool"
	LegacyOwnershipScaleSet = "runnerfoundry.scale-set"
	OwnershipRunner         = "runnerfoundry.runner"
	RunnerStateRunning      = "running"
	RunnerStateCompleted    = "completed"
	RunnerStateUnknown      = "unknown"
)
