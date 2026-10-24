package postgres

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestScopedEnqueueSerializesDemandDomainThenScopeThenSource(t *testing.T) {
	t.Parallel()
	firstScope := core.DemandScope{Adapter: "github", ID: "repository-linux"}
	otherScope := core.DemandScope{Adapter: "github", ID: "other-linux"}
	requests := []core.JobRequest{
		{Source: core.SourceReference{Adapter: "github", ID: "request-1"}, Scope: firstScope, RunnerPool: "linux", EnqueuedAt: fixedTime()},
		{Source: core.SourceReference{Adapter: "github", ID: "request-2"}, Scope: firstScope, RunnerPool: "linux", EnqueuedAt: fixedTime()},
		{Source: core.SourceReference{Adapter: "github", ID: "request-3"}, Scope: otherScope, RunnerPool: "linux", EnqueuedAt: fixedTime()},
		{Source: core.SourceReference{Adapter: "github", ID: "request-4"}, Scope: firstScope, RunnerPool: "windows", EnqueuedAt: fixedTime()},
	}
	locks := captureEnqueueLockArguments(t, requests)
	assertDemandDomainLocks(t, locks.domain)
	assertDemandScopeLocks(t, locks.scope)
}

type enqueueLockArguments struct {
	domain [][]any
	scope  [][]any
}

func captureEnqueueLockArguments(t *testing.T, requests []core.JobRequest) enqueueLockArguments {
	t.Helper()
	locks := enqueueLockArguments{
		domain: make([][]any, 0, len(requests)),
		scope:  make([][]any, 0, len(requests)),
	}
	for index, request := range requests {
		jobID := uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", index+1))
		protocol := &recordedTransaction{rows: []rowStub{rowFromValues(queuedJobValues(jobID, request))}}
		if _, err := newStore(protocol.executor()).Queue(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if protocol.statements[0] != serializeDemandDomainSQL ||
			protocol.statements[1] != serializeDemandScopeSQL ||
			protocol.statements[2] != serializeSourceSQL {
			t.Fatalf("scoped enqueue protocol = %#v", protocol.statements)
		}
		locks.domain = append(locks.domain, protocol.arguments[0])
		locks.scope = append(locks.scope, protocol.arguments[1])
	}
	return locks
}

func assertDemandDomainLocks(t *testing.T, arguments [][]any) {
	t.Helper()
	if !reflect.DeepEqual(arguments[0], arguments[1]) ||
		reflect.DeepEqual(arguments[0], arguments[3]) {
		t.Fatalf("demand-domain lock arguments = %#v", arguments)
	}
}

func assertDemandScopeLocks(t *testing.T, arguments [][]any) {
	t.Helper()
	if !reflect.DeepEqual(arguments[0], arguments[1]) ||
		reflect.DeepEqual(arguments[0], arguments[2]) ||
		!reflect.DeepEqual(arguments[0], arguments[3]) {
		t.Fatalf("demand-scope lock arguments = %#v", arguments)
	}
}
