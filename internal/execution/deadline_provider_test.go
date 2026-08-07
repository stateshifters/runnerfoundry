package execution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestDeadlineProviderAppliesValidationDeadline(t *testing.T) {
	provider, wrapped := newDeadlineTestProvider()
	if !wrapped.KnowsEndpoint("endpoint-a") || !wrapped.CanInspectEndpointForMember("endpoint-a", "member-a") {
		t.Fatal("deadline provider did not preserve endpoint ownership")
	}
	if err := wrapped.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRecordedDeadline(t, provider, "validate", providerOperationTimeout)
}

func TestDeadlineProviderAppliesRefreshDeadline(t *testing.T) {
	provider, wrapped := newDeadlineTestProvider()
	if err := wrapped.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRecordedDeadline(t, provider, "refresh", providerOperationTimeout)
}

func TestDeadlineProviderAppliesInspectionDeadlines(t *testing.T) {
	provider, wrapped := newDeadlineTestProvider()
	if _, err := wrapped.Inspect(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRecordedDeadline(t, provider, "inspect", providerOperationTimeout)
	if _, err := wrapped.InspectEndpoint(context.Background(), "endpoint-a"); err != nil {
		t.Fatal(err)
	}
	assertRecordedDeadline(t, provider, "inspect endpoint", providerOperationTimeout)
}

func TestDeadlineProviderAppliesDestructionDeadline(t *testing.T) {
	provider, wrapped := newDeadlineTestProvider()
	if err := wrapped.Destroy(context.Background(), RunnerInstance{EndpointID: "endpoint-a"}); err != nil {
		t.Fatal(err)
	}
	assertRecordedDeadline(t, provider, "destroy", providerOperationTimeout)
}

func TestDeadlineProviderAppliesStartDeadline(t *testing.T) {
	provider, wrapped := newDeadlineTestProvider()
	if err := wrapped.Start(context.Background(), StartRequest{EndpointID: "endpoint-a"}); err != nil {
		t.Fatal(err)
	}
	assertRecordedDeadline(t, provider, "start", providerStartTimeout)
}

func newDeadlineTestProvider() (*deadlineRecordingProvider, OperationDeadlineProvider) {
	provider := &deadlineRecordingProvider{endpoint: "endpoint-a", remaining: make(map[string]time.Duration)}
	return provider, WithOperationDeadlines(provider)
}

func assertRecordedDeadline(t *testing.T, provider *deadlineRecordingProvider, operation string, want time.Duration) {
	t.Helper()
	remaining := provider.remaining[operation]
	if remaining <= want-time.Second || remaining > want {
		t.Fatalf("%s deadline = %v, want approximately %v", operation, remaining, want)
	}
}

func TestDeadlineProviderPreservesEarlierCallerDeadlineAndAddsContext(t *testing.T) {
	provider := &deadlineRecordingProvider{endpoint: "endpoint-a", err: context.DeadlineExceeded, remaining: make(map[string]time.Duration)}
	wrapped := WithOperationDeadlines(provider)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := wrapped.Start(ctx, StartRequest{EndpointID: "endpoint-a"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("start error = %v, want deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), "start") || !strings.Contains(err.Error(), "endpoint-a") {
		t.Fatalf("start error = %v, want operation and endpoint context", err)
	}
	if remaining := provider.remaining["start"]; remaining <= 0 || remaining > time.Second {
		t.Fatalf("preserved deadline = %v, want at most one second", remaining)
	}
}

type deadlineRecordingProvider struct {
	endpoint  InventoryEndpointID
	err       error
	remaining map[string]time.Duration
}

func (provider *deadlineRecordingProvider) record(ctx context.Context, operation string) {
	deadline, ok := ctx.Deadline()
	if !ok {
		provider.remaining[operation] = 0
		return
	}
	provider.remaining[operation] = time.Until(deadline)
}

func (provider *deadlineRecordingProvider) EndpointID() InventoryEndpointID { return provider.endpoint }
func (provider *deadlineRecordingProvider) Validate(ctx context.Context) error {
	provider.record(ctx, "validate")
	return provider.err
}
func (provider *deadlineRecordingProvider) Start(ctx context.Context, _ StartRequest) error {
	provider.record(ctx, "start")
	return provider.err
}
func (provider *deadlineRecordingProvider) Refresh(ctx context.Context) error {
	provider.record(ctx, "refresh")
	return provider.err
}
func (provider *deadlineRecordingProvider) Inspect(ctx context.Context) ([]RunnerInstance, error) {
	provider.record(ctx, "inspect")
	return nil, provider.err
}
func (provider *deadlineRecordingProvider) InspectEndpoint(ctx context.Context, _ InventoryEndpointID) ([]RunnerInstance, error) {
	provider.record(ctx, "inspect endpoint")
	return nil, provider.err
}
func (provider *deadlineRecordingProvider) KnowsEndpoint(endpoint InventoryEndpointID) bool {
	return endpoint == provider.endpoint
}
func (provider *deadlineRecordingProvider) CanInspectEndpointForMember(endpoint InventoryEndpointID, _ core.MemberID) bool {
	return provider.KnowsEndpoint(endpoint)
}
func (provider *deadlineRecordingProvider) Destroy(ctx context.Context, _ RunnerInstance) error {
	provider.record(ctx, "destroy")
	return provider.err
}
