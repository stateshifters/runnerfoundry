package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/meza/github-scaler/internal/daemon"
)

var runDaemon = daemon.Run

func runMain(args []string, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reloadRequests, stopReloadRequests := reloadSignalRequests()
	defer stopReloadRequests()
	if err := run(ctx, args, reloadRequests); err != nil {
		if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, reloadRequests <-chan struct{}) error {
	configPath, err := parseConfigPath(args)
	if err != nil {
		return err
	}
	if err := runDaemon(ctx, daemon.Options{ConfigPath: configPath, ReloadRequests: reloadRequests}); err != nil {
		return fmt.Errorf("run github scaler: %w", err)
	}
	return nil
}

func parseConfigPath(args []string) (string, error) {
	flags := flag.NewFlagSet("github-scaler", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to scaler YAML config")
	if err := flags.Parse(args); err != nil {
		return "", fmt.Errorf("parse flags: %w", err)
	}
	if *configPath == "" {
		return "", errors.New("--config is required")
	}
	return *configPath, nil
}
