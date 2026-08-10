package github

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestTransportManagerClientWrapsSuccessAndFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("transport failed")
	registration := transport.Registration{ID: 17, Name: "linux"}
	session := &transport.Session{}
	success := composeTransportManagerClient(
		func(context.Context, string, string) (transport.Registration, error) { return registration, nil },
		func(context.Context, string) (transport.RunnerGroupID, error) { return 1, nil },
		func(context.Context, transport.Registration, string) (*transport.Session, error) { return session, nil },
		func(context.Context, transport.Registration, string) ([]byte, error) { return []byte("opaque"), nil },
		func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error) {
			return transport.PruneResult{}, nil
		},
		func(context.Context, string) error { return nil },
	)
	assertTransportClientSuccess(t, success, registration, session)

	failure := composeTransportManagerClient(
		func(context.Context, string, string) (transport.Registration, error) {
			return transport.Registration{}, wantErr
		},
		func(context.Context, string) (transport.RunnerGroupID, error) { return 0, wantErr },
		func(context.Context, transport.Registration, string) (*transport.Session, error) { return nil, wantErr },
		func(context.Context, transport.Registration, string) ([]byte, error) { return nil, wantErr },
		func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error) {
			return transport.PruneResult{}, wantErr
		},
		func(context.Context, string) error { return wantErr },
	)
	assertTransportClientFailure(t, failure, registration, wantErr)
	_ = NewTransportManagerClient((*transport.Client)(nil))
}

func TestManagedBootstrapClientRequiresCleanupBoundary(t *testing.T) {
	if err := (managedBootstrapClient{}).CleanupRunner(context.Background(), "runner"); err == nil {
		t.Fatal("missing cleanup boundary was accepted")
	}
}

func assertTransportClientSuccess(
	t *testing.T,
	client ManagerClient,
	registration transport.Registration,
	session *transport.Session,
) {
	t.Helper()
	if got, err := client.EnsureRunnerPool(context.Background(), "linux", "default"); err != nil || got != registration {
		t.Fatalf("ensure = (%#v, %v)", got, err)
	}
	if got, err := client.OpenSession(context.Background(), registration, "owner"); err != nil || got != session {
		t.Fatalf("open = (%#v, %v)", got, err)
	}
	if got, err := client.GenerateBootstrap(context.Background(), registration, "runner"); err != nil || string(got) != "opaque" {
		t.Fatalf("generate = (%q, %v)", got, err)
	}
	if _, err := client.PruneRunnerPools(context.Background(), 1, nil, nil); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if err := client.CleanupRunner(context.Background(), "runner"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

func assertTransportClientFailure(
	t *testing.T,
	client ManagerClient,
	registration transport.Registration,
	wantErr error,
) {
	t.Helper()
	if _, err := client.EnsureRunnerPool(context.Background(), "linux", "default"); !errors.Is(err, wantErr) {
		t.Fatalf("ensure error = %v", err)
	}
	if _, err := client.OpenSession(context.Background(), registration, "owner"); !errors.Is(err, wantErr) {
		t.Fatalf("open error = %v", err)
	}
	if _, err := client.GenerateBootstrap(context.Background(), registration, "runner"); !errors.Is(err, wantErr) {
		t.Fatalf("generate error = %v", err)
	}
	if _, err := client.PruneRunnerPools(context.Background(), 1, nil, nil); !errors.Is(err, wantErr) {
		t.Fatalf("prune error = %v", err)
	}
	if err := client.CleanupRunner(context.Background(), "runner"); !errors.Is(err, wantErr) {
		t.Fatalf("cleanup error = %v", err)
	}
}
