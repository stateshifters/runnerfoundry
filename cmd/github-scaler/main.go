package main

import (
	"os"
)

var exitProcess = os.Exit

func main() {
	exitProcess(runMain(os.Args[1:], os.Stdout, os.Stderr))
}
