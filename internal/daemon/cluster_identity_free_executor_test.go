package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusteredExecutorStartsIdentityFreeAssignmentWithoutInventingRequestID(t *testing.T) {
	daemon, base, provider := clusteredAssignmentExecutorFixture(t)
	base.readyAssignments = []*cluster.Assignment{{
		ID: 17, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo",
		RequestID: 0, InstallationID: 3, GitHubScaleSetID: 4, IdentityFree: true,
	}}
	state := newLifecycleClusterState(base)
	daemon.assignments = state

	daemon.executeAssignments(context.Background())

	if len(provider.started) != 1 || provider.started[0].RunnerName != "linux-17" {
		t.Fatalf("identity-free provider starts = %#v, want linux-17", provider.started)
	}
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(lifecycles) != 1 || lifecycles[0].RequestID != 0 || lifecycles[0].AssignmentID != 17 {
		t.Fatalf("identity-free lifecycle = %#v, want request ID 0 and assignment ID 17", lifecycles)
	}
}

func TestEnsureIdentityFreeAssignedRunnerReportsProviderOutcomes(t *testing.T) {
	tests := []struct {
		name      string
		provider  *identityFreeFaultProvider
		wantError string
	}{
		{name: "inspection failure", provider: &identityFreeFaultProvider{inspectErr: errors.New("inspect")}, wantError: "verify identity-free assigned runner"},
		{name: "visible after failed start", provider: &identityFreeFaultProvider{startErr: errors.New("start"), present: true}},
		{name: "failed start remains absent", provider: &identityFreeFaultProvider{startErr: errors.New("start")}, wantError: "start identity-free assigned runner"},
		{name: "successful start remains absent", provider: &identityFreeFaultProvider{}, wantError: "provider did not start"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
				Name: "linux", MaxRunners: 1, AssignmentID: 17, Provider: test.provider, JITConfig: fakeJITConfigGenerator{},
			}}
			err := ensureIdentityFreeAssignedRunner(context.Background(), reconciler)
			assertIdentityFreeOutcome(t, err, test.wantError)
		})
	}
}

func assertIdentityFreeOutcome(t *testing.T, err error, wantError string) {
	t.Helper()
	if wantError == "" && err != nil {
		t.Fatal(err)
	}
	if wantError != "" && (err == nil || !strings.Contains(err.Error(), wantError)) {
		t.Fatalf("error = %v, want %q", err, wantError)
	}
}

type identityFreeFaultProvider struct {
	listCalls  int
	startErr   error
	inspectErr error
	present    bool
}

func (provider *identityFreeFaultProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	provider.listCalls++
	if provider.listCalls > 1 {
		if provider.inspectErr != nil {
			return nil, provider.inspectErr
		}
		if provider.present {
			return []scaler.RunnerInstance{{RunnerName: "linux-17", State: scaler.RunnerStateRunning}}, nil
		}
	}
	return nil, nil
}

func (provider *identityFreeFaultProvider) Start(context.Context, scaler.RunnerStartRequest) error {
	return provider.startErr
}

func (*identityFreeFaultProvider) Destroy(context.Context, scaler.RunnerInstance) error { return nil }
