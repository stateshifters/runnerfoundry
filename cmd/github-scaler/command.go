package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/daemon"
)

var runDaemon = daemon.Run

type migrationRequest struct {
	action     string
	configPath string
	target     int32
}

type migrationStatus = cluster.MigrationStatus

var runMigration = executeMigration

func runMain(args []string, stdout io.Writer, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reloadRequests, stopReloadRequests := reloadSignalRequests()
	defer stopReloadRequests()
	if err := run(ctx, args, reloadRequests, stdout); err != nil {
		if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, reloadRequests <-chan struct{}, stdout io.Writer) error {
	if len(args) > 0 && args[0] == "migrate" {
		return runMigrationArgs(ctx, args[1:], stdout)
	}
	configPath, err := parseConfigPath(args)
	if err != nil {
		return err
	}
	if err := runDaemon(ctx, daemon.Options{ConfigPath: configPath, ReloadRequests: reloadRequests}); err != nil {
		return fmt.Errorf("run github scaler: %w", err)
	}
	return nil
}

func runMigrationArgs(ctx context.Context, args []string, stdout io.Writer) error {
	request, err := parseMigrationRequest(args)
	if err != nil {
		return err
	}
	status, err := runMigration(ctx, request)
	if err != nil {
		return fmt.Errorf("run PostgreSQL migration command: %w", err)
	}
	if request.action == "status" {
		if _, err = fmt.Fprintf(stdout, "current=%d target=%d compatibility_floor=%d known=%d..%d\n",
			status.Current, status.Target, status.CompatibilityFloor, status.Minimum, status.Target); err != nil {
			return fmt.Errorf("write migration status: %w", err)
		}
	}
	return nil
}

func parseMigrationRequest(args []string) (migrationRequest, error) {
	if len(args) == 0 {
		return migrationRequest{}, errors.New("migration command is required")
	}
	action := args[0]
	if action != "status" && action != "up" && action != "down" {
		return migrationRequest{}, fmt.Errorf("unsupported migration command %q", action)
	}
	configPath, targetText, err := parseMigrationFlags(action, args[1:])
	if err != nil {
		return migrationRequest{}, err
	}
	request := migrationRequest{action: action, configPath: configPath}
	if action == "down" {
		return parseDownMigrationRequest(request, targetText)
	}
	if targetText != "" {
		return migrationRequest{}, errors.New("--to is only valid for migrate down")
	}
	return request, nil
}

func parseMigrationFlags(action string, args []string) (configPathValue string, targetValue string, err error) {
	flags := flag.NewFlagSet("github-scaler migrate "+action, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to scaler YAML config")
	targetText := flags.String("to", "", "migration version to roll back to")
	if err := flags.Parse(args); err != nil {
		return "", "", fmt.Errorf("parse migration flags: %w", err)
	}
	if *configPath == "" {
		return "", "", errors.New("--config is required")
	}
	return *configPath, *targetText, nil
}

func parseDownMigrationRequest(request migrationRequest, targetText string) (migrationRequest, error) {
	if targetText == "" {
		return migrationRequest{}, errors.New("--to is required for migrate down")
	}
	target, err := strconv.ParseInt(targetText, 10, 32)
	if err != nil {
		return migrationRequest{}, errors.New("--to must be a migration version")
	}
	request.target = int32(target)
	return request, nil
}

func executeMigration(ctx context.Context, request migrationRequest) (migrationStatus, error) {
	configured, err := loadMigrationConfig(request.configPath)
	if err != nil {
		return migrationStatus{}, err
	}
	store := cluster.NewStoreFromConfig(*configured.State.Postgres)
	return executeStoreMigration(ctx, store, request)
}

func loadMigrationConfig(configPath string) (config.Config, error) {
	configRoot, err := os.OpenRoot(filepath.Dir(configPath))
	if err != nil {
		return config.Config{}, fmt.Errorf("open config directory: %w", err)
	}
	configFile, err := configRoot.Open(filepath.Base(configPath))
	if err != nil {
		return config.Config{}, errors.Join(fmt.Errorf("open config: %w", err), configRoot.Close())
	}
	configContents, readErr := io.ReadAll(configFile)
	closeErr := errors.Join(configFile.Close(), configRoot.Close())
	if err = errors.Join(readErr, closeErr); err != nil {
		return config.Config{}, fmt.Errorf("read config: %w", err)
	}
	configured, err := config.Load(bytes.NewReader(configContents))
	if err != nil {
		return config.Config{}, fmt.Errorf("load migration config: %w", err)
	}
	if configured.State.Postgres == nil {
		return config.Config{}, errors.New("PostgreSQL clustered state is required")
	}
	return configured, nil
}

func executeStoreMigration(ctx context.Context, store *cluster.Store, request migrationRequest) (migrationStatus, error) {
	var err error
	switch request.action {
	case "status":
		status, statusErr := store.MigrationStatus(ctx)
		return status, wrapMigrationError(statusErr)
	case "up":
		if err = store.Migrate(ctx); err != nil {
			return migrationStatus{}, fmt.Errorf("migrate PostgreSQL up: %w", err)
		}
		status, statusErr := store.MigrationStatus(ctx)
		return status, wrapMigrationError(statusErr)
	case "down":
		if err = store.MigrateDown(ctx, request.target, time.Now(), cluster.NodeLifetime); err != nil {
			return migrationStatus{}, fmt.Errorf("migrate PostgreSQL down: %w", err)
		}
		status, statusErr := store.MigrationStatus(ctx)
		return status, wrapMigrationError(statusErr)
	default:
		return migrationStatus{}, errors.New("unsupported migration command")
	}
}

func wrapMigrationError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("read migration status: %w", err)
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
