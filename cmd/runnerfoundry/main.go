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

type processLifecycle struct {
	notifyContext func(context.Context, ...os.Signal) (context.Context, context.CancelFunc)
	notifySignals func(chan<- os.Signal, ...os.Signal)
	stopSignals   func(chan<- os.Signal)
}

type processRuntime struct {
	args         []string
	stdout       io.Writer
	stderr       io.Writer
	runDaemon    daemonCommand
	runMigration migrationCommand
	lifecycle    processLifecycle
	exit         func(int)
}

var productionProcess = processRuntime{
	args:         os.Args[1:],
	stdout:       os.Stdout,
	stderr:       os.Stderr,
	runDaemon:    daemon.Run,
	runMigration: runPostgreSQLMigration,
	lifecycle: processLifecycle{
		notifyContext: signal.NotifyContext,
		notifySignals: signal.Notify,
		stopSignals:   signal.Stop,
	},
	exit: os.Exit,
}

func main() {
	productionProcess.exit(runMain(
		productionProcess.args,
		productionProcess.stdout,
		productionProcess.stderr,
		productionProcess.runDaemon,
		productionProcess.runMigration,
		productionProcess.lifecycle,
	))
}

func runMain(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	runDaemon daemonCommand,
	runMigration migrationCommand,
	lifecycle processLifecycle,
) int {
	ctx, stop := lifecycle.notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reloadRequests, stopReloadRequests := reloadSignalRequests(lifecycle.notifySignals, lifecycle.stopSignals)
	defer stopReloadRequests()
	if err := executeCommand(ctx, args, reloadRequests, stdout, stderr, runDaemon, runMigration); err != nil {
		if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
			return 1
		}
		return 1
	}
	return 0
}
