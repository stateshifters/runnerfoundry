package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/meza/RunnerFoundry/internal/daemon"
)

var exitProcess = os.Exit

func main() {
	exitProcess(runMain(os.Args[1:], os.Stdout, os.Stderr, daemon.Run, runPostgreSQLMigration))
}

func runMain(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	runDaemon daemonCommand,
	runMigration migrationCommand,
) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reloadRequests, stopReloadRequests := reloadSignalRequests()
	defer stopReloadRequests()
	if err := executeCommand(ctx, args, reloadRequests, stdout, stderr, runDaemon, runMigration); err != nil {
		if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
			return 1
		}
		return 1
	}
	return 0
}
