package runneridentitycontract

// Identity is a backend-neutral test input for runner selection.
type Identity struct {
	RequestID  int64
	RunnerName string
}

// Outcome names the observable result expected from a contract case.
type Outcome string

const (
	Success   Outcome = "success"
	Invalid   Outcome = "invalid"
	NotFound  Outcome = "not_found"
	Mismatch  Outcome = "mismatch"
	Invariant Outcome = "invariant"
)

// Case describes one shared runner-selection behavior.
type Case struct {
	Name     string
	Runners  []Identity
	Identity Identity
	Outcome  Outcome
}

// Cases returns the required identity modes and failure outcomes for every backend.
func Cases() []Case {
	return []Case{
		{Name: "request ID only", Runners: []Identity{{RequestID: 1, RunnerName: "runner-one"}},
			Identity: Identity{RequestID: 1}, Outcome: Success},
		{Name: "runner name only", Runners: []Identity{{RequestID: 1, RunnerName: "runner-one"}},
			Identity: Identity{RunnerName: "runner-one"}, Outcome: Success},
		{Name: "matching pair", Runners: []Identity{{RequestID: 1, RunnerName: "runner-one"}},
			Identity: Identity{RequestID: 1, RunnerName: "runner-one"}, Outcome: Success},
		{Name: "mismatched pair", Runners: []Identity{
			{RequestID: 1, RunnerName: "runner-one"}, {RequestID: 2, RunnerName: "runner-two"}},
			Identity: Identity{RequestID: 1, RunnerName: "runner-two"}, Outcome: Mismatch},
		{Name: "empty identity", Identity: Identity{}, Outcome: Invalid},
		{Name: "not found", Runners: []Identity{{RequestID: 1, RunnerName: "runner-one"}},
			Identity: Identity{RequestID: 404}, Outcome: NotFound},
		{Name: "duplicate request ID", Runners: []Identity{
			{RequestID: 1, RunnerName: "runner-one"}, {RequestID: 1, RunnerName: "runner-two"}},
			Identity: Identity{RequestID: 1}, Outcome: Invariant},
	}
}
