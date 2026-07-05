//go:build !windows

package main

import (
	"os"
	"syscall"
	"testing"
	"time"
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

func TestReloadSignalRequestsCanStop(t *testing.T) {
	_, stop := reloadSignalRequests()
	stop()
	time.Sleep(time.Millisecond)
}
