package orchard

import (
	"context"
	"testing"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

const runnerName core.RunnerName = "runnerfoundry-v2-AQAAAAAAAAAAAAAAAAAAAA-AgAAAAAAAAAAAAAAAAAAAA"

func TestInventorySnapshotRetainsCompleteViewUntilRefresh(t *testing.T) {
	api := &fakeAPI{records: []v1.VM{ownedVM("cG9vbA")}}
	provider := testProvider(t, api, nil)
	first := inspectOrchardSnapshot(t, provider)
	if len(first) != 1 || first[0].RunnerPool != "pool" {
		t.Fatalf("inventory=%+v", first)
	}
	api.records = []v1.VM{}
	first[0].RunnerPool = "mutated"
	cached := inspectOrchardSnapshot(t, provider)
	if len(cached) != 1 || cached[0].RunnerPool != "pool" || api.lists != 1 {
		t.Fatalf("snapshot=%+v lists=%d", cached, api.lists)
	}
	fresh, err := provider.InspectEndpoint(context.Background(), provider.EndpointID())
	if err != nil || len(fresh) != 0 {
		t.Fatalf("fresh=%+v error=%v", fresh, err)
	}
	if refreshError := provider.Refresh(context.Background()); refreshError != nil {
		t.Fatal(refreshError)
	}
	if cached = inspectOrchardSnapshot(t, provider); len(cached) != 0 {
		t.Fatalf("refreshed=%+v", cached)
	}
}

func TestSharedControllerCanInspectAnotherMember(t *testing.T) {
	provider := testProvider(t, &fakeAPI{}, nil)
	if !provider.CanInspectEndpointForMember(provider.EndpointID(), "another-member") {
		t.Fatal("shared controller must be inspectable by another member")
	}
}

func inspectOrchardSnapshot(t *testing.T, provider *Provider) []execution.RunnerInstance {
	t.Helper()
	instances, err := provider.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return instances
}

type fakeAPI struct {
	records     []v1.VM
	lists       int
	listError   error
	created     []*v1.VM
	createError error
	deleted     []string
	deleteError error
}

func (api *fakeAPI) List(context.Context) ([]v1.VM, error) {
	api.lists++
	return api.records, api.listError
}
func (api *fakeAPI) Create(_ context.Context, vm *v1.VM) error {
	api.created = append(api.created, vm)
	return api.createError
}
func (api *fakeAPI) Delete(_ context.Context, name string) error {
	api.deleted = append(api.deleted, name)
	return api.deleteError
}
