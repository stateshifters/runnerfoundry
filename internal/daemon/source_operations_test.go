package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestSourceOperationsResolverRoutesByStableNamespace(t *testing.T) {
	first := &sourceBootstrapProviderFake{}
	second := &reportingSourceBootstrapProviderFake{sourceBootstrapProviderFake: sourceBootstrapProviderFake{}}
	resolver, err := newSourceOperationsResolver([]sourceOperationsRegistration{
		{adapter: "first", operations: first},
		{adapter: "second", operations: second},
	})
	if err != nil {
		t.Fatalf("newSourceOperationsResolver() error = %v", err)
	}

	actual, err := resolver.ResolveSourceOperations("second")
	if err != nil || actual.RunnerBootstrapProvider != second {
		t.Fatalf("ResolveSourceOperations() = (%T, %v)", actual, err)
	}
	if _, supported := actual.RunnerBootstrapProvider.(core.ProvisioningFailureReporter); !supported {
		t.Fatal("resolved source lost its optional provisioning-failure capability")
	}
}

func TestSourceOperationsResolverRejectsDuplicateAndUnknownNamespaces(t *testing.T) {
	provider := &sourceBootstrapProviderFake{}
	if _, err := newSourceOperationsResolver([]sourceOperationsRegistration{
		{adapter: "duplicate", operations: provider},
		{adapter: "duplicate", operations: provider},
	}); err == nil {
		t.Fatal("duplicate source namespace was accepted")
	}

	resolver, err := newSourceOperationsResolver([]sourceOperationsRegistration{{
		adapter: "known", operations: provider,
	}})
	if err != nil {
		t.Fatalf("newSourceOperationsResolver() error = %v", err)
	}
	if _, err := resolver.ResolveSourceOperations("unknown"); !errors.Is(err, errUnknownSourceAdapter) {
		t.Fatalf("unknown source error = %v", err)
	}
}

func TestSourceOperationsResolverRejectsIncompleteRegistrations(t *testing.T) {
	provider := &sourceBootstrapProviderFake{}
	var typedNil *sourceBootstrapProviderFake
	for _, registration := range []sourceOperationsRegistration{
		{operations: provider},
		{adapter: "missing-operations"},
		{adapter: "typed-nil-operations", operations: typedNil},
	} {
		if _, err := newSourceOperationsResolver([]sourceOperationsRegistration{registration}); err == nil {
			t.Fatalf("registration %#v was accepted", registration)
		}
	}
}

type sourceBootstrapProviderFake struct{}

func (*sourceBootstrapProviderFake) GenerateBootstrap(
	context.Context,
	core.Job,
	core.RunnerName,
) (core.Bootstrap, error) {
	return core.NewBootstrap([]byte("opaque")), nil
}

func (*sourceBootstrapProviderFake) CleanupBootstrap(context.Context, core.Job, core.RunnerName) error {
	return nil
}

type reportingSourceBootstrapProviderFake struct{ sourceBootstrapProviderFake }

func (*reportingSourceBootstrapProviderFake) ReportProvisioningFailure(
	context.Context,
	core.Job,
	core.RunnerName,
) error {
	return nil
}
