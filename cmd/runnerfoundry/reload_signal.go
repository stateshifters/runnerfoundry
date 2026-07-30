package main

import (
	"os"
	"syscall"
)

func reloadSignalRequests(
	notifySignals func(chan<- os.Signal, ...os.Signal),
	stopSignals func(chan<- os.Signal),
) (<-chan struct{}, func()) {
	signals := make(chan os.Signal, 1)
	reloadRequests := make(chan struct{}, 1)
	notifySignals(signals, syscall.SIGHUP)
	stop := func() {
		stopSignals(signals)
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
