//go:build !coverage

package main

import (
	"os"
)

func main() {
	os.Exit(runMain(os.Args[1:], os.Stderr))
}
