package orchard

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestStartIsolatesNativeHostProcessesAndNetworkPolicies(t *testing.T) {
	provider, api := nativeSpecProvider(t, `{"image":"base","hostProcesses":[{"name":"proxy","program":"proxy","args":["original"],"env":{"MODE":"original"}}],"netSoftnetAllow":["10.0.0.0/8"],"netSoftnetBlock":["192.168.0.0/16"]}`)
	first := createBeforeGuestFailure(t, provider, api)
	first.HostProcesses[0].Args[0] = "mutated"
	first.HostProcesses[0].Env["MODE"] = "mutated"
	first.NetSoftnetAllow[0] = "mutated"
	first.NetSoftnetBlock[0] = "mutated"
	second := createBeforeGuestFailure(t, provider, api)
	expected := []v1.HostProcess{{Name: "proxy", Program: "proxy", Args: []string{"original"}, Env: map[string]string{"MODE": "original"}}}
	if !reflect.DeepEqual(second.HostProcesses, expected) {
		t.Fatalf("host processes=%+v", second.HostProcesses)
	}
	if !slices.Equal(second.NetSoftnetAllow, []string{"10.0.0.0/8"}) || !slices.Equal(second.NetSoftnetBlock, []string{"192.168.0.0/16"}) {
		t.Fatal("network policy mutated between starts")
	}
}

func TestStartIsolatesNativeEndpointTargetsAndPortRanges(t *testing.T) {
	provider, api := nativeSpecProvider(t, `{"image":"base","endpoints":[{"name":"ssh","protocol":"tcp","target":{"vm":{"port":22}},"workerPortRange":{"min":10000,"max":10100}},{"name":"web","protocol":"tcp","target":{"vm":{"port":80}}}]}`)
	first := createBeforeGuestFailure(t, provider, api)
	first.Endpoints[0].Target.VM.Port = 1234
	first.Endpoints[0].WorkerPortRange.Min = 1234
	second := createBeforeGuestFailure(t, provider, api)
	if second.Endpoints[0].Target.VM.Port != 22 || second.Endpoints[0].WorkerPortRange.Min != 10000 || second.Endpoints[0].WorkerPortRange.Max != 10100 || second.Endpoints[1].Target.VM.Port != 80 {
		t.Fatalf("endpoints=%+v", second.Endpoints)
	}
}

func TestStartIsolatesNativeStartupScriptAndResources(t *testing.T) {
	provider, api := nativeSpecProvider(t, `{"image":"base","resources":{"org.cirruslabs.tart.vm":1},"startup_script":{"script_content":"prepare","env":{"MODE":"original"}}}`)
	first := createBeforeGuestFailure(t, provider, api)
	first.StartupScript.ScriptContent = "mutated"
	first.StartupScript.Env["MODE"] = "mutated"
	first.Resources["org.cirruslabs.tart.vm"] = 5
	second := createBeforeGuestFailure(t, provider, api)
	expected := &v1.VMScript{ScriptContent: "prepare", Env: map[string]string{"MODE": "original"}}
	if !reflect.DeepEqual(second.StartupScript, expected) || second.Resources["org.cirruslabs.tart.vm"] != 1 {
		t.Fatalf("startup=%+v resources=%+v", second.StartupScript, second.Resources)
	}
}

func nativeSpecProvider(t *testing.T, spec string) (*Provider, *fakeAPI) {
	t.Helper()
	api := &fakeAPI{}
	provider, err := NewProvider(api, &fakeConnector{err: errors.New("guest unavailable")}, Config{ControllerURL: "https://orchard.example", VMSpec: []byte(spec)})
	if err != nil {
		t.Fatal(err)
	}
	return provider, api
}

func createBeforeGuestFailure(t *testing.T, provider *Provider, api *fakeAPI) *v1.VM {
	t.Helper()
	if err := provider.Start(context.Background(), execution.StartRequest{RunnerPool: "pool", RunnerName: runnerName}); err == nil {
		t.Fatal("unavailable guest accepted")
	}
	if len(api.created) == 0 {
		t.Fatal("VM not created before guest connection")
	}
	return api.created[len(api.created)-1]
}
