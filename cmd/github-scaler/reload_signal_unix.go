//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func reloadSignalRequests() (<-chan struct{}, func()) {
	signals := make(chan os.Signal, 1)
	reloadRequests := make(chan struct{}, 1)
	signal.Notify(signals, syscall.SIGHUP)
	stop := func() {
		signal.Stop(signals)
		close(signals)
	}
	go forwardReloadSignals(signals, reloadRequests)
	return reloadRequests, stop
}

func forwardReloadSignals(signals <-chan os.Signal, reloadRequests chan<- struct{}) {
	for range signals {
		select {
		case reloadRequests <- struct{}{}:
		default:
		}
	}
}
