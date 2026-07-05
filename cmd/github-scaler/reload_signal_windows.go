//go:build windows

package main

func reloadSignalRequests() (<-chan struct{}, func()) {
	return nil, func() {}
}
