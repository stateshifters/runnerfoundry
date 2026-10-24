package orchard

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

type Config struct {
	ControllerURL      string
	ServiceAccount     string
	Token              string
	TrustedCertificate []byte
	VMSpec             []byte
}

type API interface {
	Create(ctx context.Context, vm *v1.VM) error
	List(ctx context.Context) ([]v1.VM, error)
	Delete(ctx context.Context, name string) error
}

type Provider struct {
	api           API
	guest         GuestConnector
	endpoint      execution.InventoryEndpointID
	spec          v1.VM
	mutex         sync.Mutex
	snapshot      []execution.RunnerInstance
	snapshotReady bool
	snapshotError error
	after         func(time.Duration) <-chan time.Time
}

func NewProvider(api API, guest GuestConnector, config Config) (*Provider, error) {
	address, err := normalizeURL(config.ControllerURL)
	if err != nil {
		return nil, err
	}
	spec, err := validateSpec(config.VMSpec)
	if err != nil {
		return nil, err
	}
	return &Provider{api: api, guest: guest, endpoint: execution.InventoryEndpointID("orchard:" + address), spec: spec, after: time.After}, nil
}

func (provider *Provider) EndpointID() execution.InventoryEndpointID { return provider.endpoint }
func (provider *Provider) KnowsEndpoint(endpoint execution.InventoryEndpointID) bool {
	return endpoint == provider.endpoint
}
func (provider *Provider) CanInspectEndpointForMember(endpoint execution.InventoryEndpointID, _ core.MemberID) bool {
	return provider.KnowsEndpoint(endpoint)
}
func (provider *Provider) Validate(ctx context.Context) error {
	_, err := provider.readInventory(ctx)
	return err
}

func (provider *Provider) Refresh(ctx context.Context) error {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	return provider.replaceSnapshot(ctx)
}
func (provider *Provider) replaceSnapshot(ctx context.Context) error {
	instances, err := provider.readInventory(ctx)
	provider.snapshot = instances
	provider.snapshotReady = true
	provider.snapshotError = err
	return err
}
func (provider *Provider) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	if !provider.snapshotReady {
		if err := provider.replaceSnapshot(ctx); err != nil {
			return nil, err
		}
	}
	return append([]execution.RunnerInstance(nil), provider.snapshot...), provider.snapshotError
}
func (provider *Provider) InspectEndpoint(ctx context.Context, endpoint execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	if !provider.KnowsEndpoint(endpoint) {
		return nil, errors.New("unknown Orchard endpoint")
	}
	return provider.readInventory(ctx)
}
func (provider *Provider) readInventory(ctx context.Context) ([]execution.RunnerInstance, error) {
	records, err := provider.api.List(ctx)
	if err != nil {
		return nil, errors.New("list Orchard VMs failed")
	}
	if records == nil {
		return nil, errors.New("orchard inventory must be a complete array")
	}
	instances := make([]execution.RunnerInstance, 0)
	seen := make(map[string]bool)
	for _, record := range records {
		if record.Name == "" || seen[record.Name] {
			return nil, errors.New("invalid Orchard inventory identity")
		}
		seen[record.Name] = true
		instance, owned, err := parseName(record.Name)
		if err != nil {
			return nil, err
		}
		if !owned {
			continue
		}
		instance.EndpointID = provider.endpoint
		instance.State = runnerState(record)
		instances = append(instances, instance)
	}
	return instances, nil
}

func runnerState(vm v1.VM) execution.RunnerState {
	if vm.Generation != vm.ObservedGeneration {
		return execution.RunnerStateUnknown
	}
	if vm.Status == v1.VMStatusFailed {
		return execution.RunnerStateCompleted
	}
	if vm.Status != v1.VMStatusRunning {
		return execution.RunnerStateUnknown
	}
	if v1.ConditionIsFalse(vm.Conditions, v1.ConditionTypeRunning) {
		return execution.RunnerStateCompleted
	}
	if v1.ConditionIsTrue(vm.Conditions, v1.ConditionTypeRunning) {
		return execution.RunnerStateRunning
	}
	return execution.RunnerStateUnknown
}

const ownershipPrefix = "runnerfoundry-orchard-"
const versionPrefix = ownershipPrefix + "v1-"
const maximumVMNameLength = 190

func parseName(name string) (execution.RunnerInstance, bool, error) {
	if !strings.HasPrefix(name, ownershipPrefix) {
		return execution.RunnerInstance{}, false, nil
	}
	encoded, versionOK := strings.CutPrefix(name, versionPrefix)
	poolToken, runner, found := strings.Cut(encoded, "-runnerfoundry-v2-")
	pool, err := base64.RawURLEncoding.Strict().DecodeString(poolToken)
	runnerName := core.RunnerName("runnerfoundry-v2-" + runner)
	if !versionOK || !found || err != nil || len(pool) == 0 || !core.IsRunnerFoundryRunnerName(runnerName) || len(name) > maximumVMNameLength {
		return execution.RunnerInstance{}, true, errors.New("malformed owned Orchard VM name")
	}
	return execution.RunnerInstance{ID: name, RunnerPool: core.RunnerPool(pool), RunnerName: runnerName}, true, nil
}
func ownedName(pool core.RunnerPool, runner core.RunnerName) (string, error) {
	name := versionPrefix + base64.RawURLEncoding.EncodeToString([]byte(pool)) + "-" + string(runner)
	_, _, err := parseName(name)
	return name, err
}

func (provider *Provider) Start(ctx context.Context, request execution.StartRequest) error {
	name, err := ownedName(request.RunnerPool, request.RunnerName)
	if err != nil {
		return err
	}
	if request.EndpointID != "" && !provider.KnowsEndpoint(request.EndpointID) {
		return errors.New("foreign Orchard launch endpoint")
	}
	vm := cloneSpec(provider.spec)
	vm.Name = name
	if err := provider.api.Create(ctx, &vm); err != nil {
		return errors.New("create Orchard runner failed")
	}
	if err := provider.deliver(ctx, name, request.Bootstrap.Content()); err != nil {
		return fmt.Errorf("initialize Orchard runner: %w", err)
	}
	return nil
}
func (provider *Provider) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	owned, valid, err := parseName(instance.ID)
	if err != nil || !valid || instance.EndpointID != provider.endpoint || owned.RunnerName != instance.RunnerName || owned.RunnerPool != instance.RunnerPool {
		return errors.New("refuse foreign or malformed Orchard runner destruction")
	}
	if err := provider.api.Delete(ctx, instance.ID); err != nil {
		return errors.New("remove Orchard runner failed")
	}
	return nil
}

var _ execution.ExecutionProvider = (*Provider)(nil)
