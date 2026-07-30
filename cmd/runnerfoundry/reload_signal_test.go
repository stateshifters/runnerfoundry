package main

import (
	"os"
	"syscall"
	"testing"
)

func TestForwardReloadSignalsCoalescesRequests(t *testing.T) {
	signals := make(chan os.Signal, 2)
	reloadRequests := make(chan struct{}, 1)
	signals <- syscall.SIGHUP
	signals <- syscall.SIGHUP
	close(signals)

	forwardReloadSignals(signals, reloadRequests)
	select {
	case <-reloadRequests:
	default:
		t.Fatal("expected reload request")
	}
	select {
	case <-reloadRequests:
		t.Fatal("expected coalesced reload request")
	default:
	}
}

func TestReloadSignalRequestsRegistersAndStopsWithoutOperatingSystemSignals(t *testing.T) {
	var registered chan<- os.Signal
	stopped := false
	reloadRequests, stop := reloadSignalRequests(
		func(signals chan<- os.Signal, requested ...os.Signal) {
			registered = signals
			if len(requested) != 1 || requested[0] != syscall.SIGHUP {
				t.Fatalf("requested signals = %#v", requested)
			}
		},
		func(signals chan<- os.Signal) {
			stopped = signals == registered
		},
	)
	registered <- syscall.SIGHUP
	select {
	case <-reloadRequests:
	case <-t.Context().Done():
		t.Fatal("reload signal was not forwarded")
	}
	stop()
	if !stopped {
		t.Fatal("signal registration was not stopped")
	}
}
