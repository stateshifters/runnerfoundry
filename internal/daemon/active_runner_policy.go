package daemon

import "fmt"

type ActiveRunnerPolicy string

const (
	LeaveActiveRunners     ActiveRunnerPolicy = "leave"
	TerminateActiveRunners ActiveRunnerPolicy = "terminate"
)

func resolveActiveRunnerPolicy(policy ActiveRunnerPolicy) (ActiveRunnerPolicy, error) {
	if policy == "" {
		return LeaveActiveRunners, nil
	}
	if policy != LeaveActiveRunners && policy != TerminateActiveRunners {
		return "", fmt.Errorf("active runner policy must be %q or %q", LeaveActiveRunners, TerminateActiveRunners)
	}
	return policy, nil
}
