package daemon

import (
	"context"
	"testing"
	"time"
)

func TestClusterMessageObservedAtFallsBackToCurrentUTC(t *testing.T) {
	before := time.Now().UTC()
	observedAt := (clusterMessageHandler{}).observedAt()
	after := time.Now().UTC()

	if observedAt.Before(before) || observedAt.After(after) || observedAt.Location() != time.UTC {
		t.Fatalf("fallback observation time = %s, range %s..%s", observedAt, before, after)
	}
}

func TestTermContextFactoryReturnsCanceledRuntimeForEndedTerm(t *testing.T) {
	termContext, endTerm := context.WithCancel(context.Background())
	endTerm()
	runtimeContext, stopRuntime := newTermContextFactory(termContext)(context.Background())

	select {
	case <-runtimeContext.Done():
	default:
		t.Fatal("runtime context remained active after term ended")
	}
	stopRuntime()
	stopRuntime()
}
