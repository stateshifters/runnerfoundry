package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/daemon"
)

func TestParseConfigPathRequiresConfigFlag(t *testing.T) {
	if _, err := parseConfigPath(nil); err == nil {
		t.Fatal("expected missing config error")
	}
}

func TestParseConfigPathReturnsConfigFlag(t *testing.T) {
	configPath, err := parseConfigPath([]string{"--config", "config.yaml"})
	if err != nil {
		t.Fatalf("expected config path, got %v", err)
	}
	if configPath != "config.yaml" {
		t.Fatalf("expected config.yaml, got %q", configPath)
	}
}

func TestParseConfigPathSurfacesFlagErrors(t *testing.T) {
	if _, err := parseConfigPath([]string{"--missing"}); err == nil {
		t.Fatal("expected flag parse error")
	}
}

func TestRunDelegatesToDaemon(t *testing.T) {
	originalRunDaemon := runDaemon
	defer func() {
		runDaemon = originalRunDaemon
	}()
	var capturedOptions daemon.Options
	runDaemon = func(ctx context.Context, options daemon.Options) error {
		capturedOptions = options
		return nil
	}

	reloadRequests := make(chan struct{})
	if err := run(context.Background(), []string{"--config", "config.yaml"}, reloadRequests, io.Discard); err != nil {
		t.Fatalf("expected run success, got %v", err)
	}
	if capturedOptions.ConfigPath != "config.yaml" {
		t.Fatalf("expected daemon config path, got %#v", capturedOptions)
	}
	if capturedOptions.ReloadRequests != reloadRequests {
		t.Fatal("expected reload requests channel to be passed to daemon")
	}
}

func TestRunSurfacesParseAndDaemonErrors(t *testing.T) {
	if err := run(context.Background(), nil, nil, io.Discard); err == nil {
		t.Fatal("expected parse error")
	}

	originalRunDaemon := runDaemon
	defer func() {
		runDaemon = originalRunDaemon
	}()
	runDaemon = func(ctx context.Context, options daemon.Options) error {
		return errors.New("daemon failed")
	}
	if err := run(context.Background(), []string{"--config", "config.yaml"}, nil, io.Discard); err == nil {
		t.Fatal("expected daemon error")
	}
}

func TestRunMainReportsExitStatus(t *testing.T) {
	originalRunDaemon := runDaemon
	defer func() {
		runDaemon = originalRunDaemon
	}()

	runDaemon = func(ctx context.Context, options daemon.Options) error {
		return nil
	}
	var stderr bytes.Buffer
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, &stderr); exitCode != 0 {
		t.Fatalf("expected success exit, got %d", exitCode)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr, got %q", stderr.String())
	}

	runDaemon = func(ctx context.Context, options daemon.Options) error {
		return errors.New("daemon failed")
	}
	if exitCode := runMain([]string{"--config", "config.yaml"}, io.Discard, &stderr); exitCode != 1 {
		t.Fatalf("expected failure exit, got %d", exitCode)
	}
	if !strings.Contains(stderr.String(), "daemon failed") {
		t.Fatalf("expected daemon error on stderr, got %q", stderr.String())
	}
}

func TestRunMainHandlesStderrFailures(t *testing.T) {
	if exitCode := runMain(nil, io.Discard, failingWriter{}); exitCode != 1 {
		t.Fatalf("expected failure exit, got %d", exitCode)
	}
}

func TestRunMigrationCommands(t *testing.T) {
	originalRunMigration := runMigration
	defer func() { runMigration = originalRunMigration }()
	requests := make([]migrationRequest, 0, 3)
	runMigration = func(_ context.Context, request migrationRequest) (migrationStatus, error) {
		requests = append(requests, request)
		return migrationStatus{Current: 1, Target: 2, CompatibilityFloor: 1, Minimum: 0}, nil
	}
	var output bytes.Buffer
	commands := [][]string{
		{"migrate", "status", "--config", "config.yaml"},
		{"migrate", "up", "--config", "config.yaml"},
		{"migrate", "down", "--to", "1", "--config", "config.yaml"},
	}
	for _, args := range commands {
		if err := run(context.Background(), args, nil, &output); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 3 || requests[0].action != "status" || requests[1].action != "up" || requests[2].action != "down" || requests[2].target != 1 {
		t.Fatalf("migration requests = %#v", requests)
	}
	if output.String() != "current=1 target=2 compatibility_floor=1 known=0..2\n" {
		t.Fatalf("migration status output = %q", output.String())
	}
}

func TestRunMigrationCommandsRejectInvalidArgumentsAndFailures(t *testing.T) {
	for _, args := range [][]string{
		{"migrate"}, {"migrate", "unknown", "--config", "config.yaml"},
		{"migrate", "status"}, {"migrate", "down", "--config", "config.yaml"},
		{"migrate", "down", "--to", "invalid", "--config", "config.yaml"},
		{"migrate", "up", "--to", "1", "--config", "config.yaml"},
		{"migrate", "up", "--unknown", "value", "--config", "config.yaml"},
	} {
		if err := run(context.Background(), args, nil, io.Discard); err == nil {
			t.Fatalf("expected %q to fail", args)
		}
	}
	originalRunMigration := runMigration
	defer func() { runMigration = originalRunMigration }()
	runMigration = func(context.Context, migrationRequest) (migrationStatus, error) {
		return migrationStatus{}, errors.New("migration failed")
	}
	if err := run(context.Background(), []string{"migrate", "up", "--config", "config.yaml"}, nil, io.Discard); err == nil {
		t.Fatal("expected migration failure")
	}
	runMigration = func(context.Context, migrationRequest) (migrationStatus, error) {
		return migrationStatus{Current: 1, Target: 1}, nil
	}
	if err := run(context.Background(), []string{"migrate", "status", "--config", "config.yaml"}, nil, failingWriter{}); err == nil {
		t.Fatal("expected migration status output failure")
	}
}

func TestExecuteMigrationRunsEmbeddedPostgreSQLLifecycle(t *testing.T) {
	dsn := commandTestPostgreSQL(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configText := fmt.Sprintf(`github:
  app:
    app_id: 1
    private_key_file: unused.pem
state:
  postgres:
    role: postgres
    dsn: %s
scale_sets:
  - name: linux
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
`, dsn)
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	status, err := executeMigration(ctx, migrationRequest{action: "up", configPath: configPath})
	if err != nil || status.Current != 1 {
		t.Fatalf("up status = %#v, %v", status, err)
	}
	status, err = executeMigration(ctx, migrationRequest{action: "status", configPath: configPath})
	if err != nil || status.Current != 1 {
		t.Fatalf("status = %#v, %v", status, err)
	}
	status, err = executeMigration(ctx, migrationRequest{action: "down", configPath: configPath, target: 0})
	if err != nil || status.Current != 0 {
		t.Fatalf("down status = %#v, %v", status, err)
	}
	if _, err = executeMigration(ctx, migrationRequest{action: "down", configPath: configPath, target: 1}); err == nil {
		t.Fatal("expected invalid rollback target")
	}
	if _, err = executeMigration(ctx, migrationRequest{action: "unsupported", configPath: configPath}); err == nil {
		t.Fatal("expected unsupported migration action")
	}
}

func TestExecuteMigrationRejectsInvalidConfiguration(t *testing.T) {
	missingDirectory := filepath.Join(t.TempDir(), "missing", "config.yaml")
	if _, err := executeMigration(context.Background(), migrationRequest{action: "status", configPath: missingDirectory}); err == nil {
		t.Fatal("expected missing config directory error")
	}
	if _, err := executeMigration(context.Background(), migrationRequest{action: "status", configPath: "missing-config.yaml"}); err == nil {
		t.Fatal("expected missing config error")
	}
	if _, err := executeMigration(context.Background(), migrationRequest{action: "status", configPath: t.TempDir()}); err == nil {
		t.Fatal("expected config directory read error")
	}
	invalidPath := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalidPath, []byte("github: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := executeMigration(context.Background(), migrationRequest{action: "status", configPath: invalidPath}); err == nil {
		t.Fatal("expected invalid config error")
	}
	privateKeyPath := filepath.ToSlash(filepath.Join(t.TempDir(), "unused.pem"))
	sqlitePath := filepath.Join(t.TempDir(), "sqlite.yaml")
	sqliteConfig := fmt.Sprintf(`github:
  app:
    app_id: 1
    private_key_file: %s
state:
  path: state.db
scale_sets:
  - name: linux
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
`, privateKeyPath)
	if err := os.WriteFile(sqlitePath, []byte(sqliteConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := executeMigration(context.Background(), migrationRequest{action: "status", configPath: sqlitePath}); err == nil {
		t.Fatal("expected PostgreSQL requirement error")
	}
}

func TestExecuteStoreMigrationReportsConnectionFailures(t *testing.T) {
	store := cluster.NewStore(cluster.ConnectionSettings{
		DSN: "postgres://postgres:secret@127.0.0.1:1/postgres?connect_timeout=1", Role: "github_scaler",
	})
	for _, action := range []string{"status", "up", "down"} {
		if _, err := executeStoreMigration(context.Background(), store, migrationRequest{action: action}); err == nil {
			t.Fatalf("expected %s connection failure", action)
		}
	}
	if err := wrapMigrationError(errors.New("failed")); err == nil {
		t.Fatal("expected wrapped migration error")
	}
}

func commandTestPostgreSQL(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("unexpected PostgreSQL test address: %v", listener.Addr())
	}
	var port uint32
	if _, err = fmt.Sscan(portText, &port); err != nil || port == 0 || port > 65535 {
		t.Fatalf("unexpected PostgreSQL test port: %q", portText)
	}
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	database := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Port(port).
		RuntimePath(filepath.Join(t.TempDir(), "postgres")).
		Logger(io.Discard))
	if err = database.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if stopErr := database.Stop(); stopErr != nil {
			t.Errorf("stop embedded PostgreSQL: %v", stopErr)
		}
	})
	return fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable&connect_timeout=1", port)
}

func TestMainExitsWithRunMainStatus(t *testing.T) {
	originalArgs := os.Args
	originalExitProcess := exitProcess
	defer func() {
		os.Args = originalArgs
		exitProcess = originalExitProcess
	}()
	os.Args = []string{"github-scaler"}
	var exitCode int
	exitProcess = func(code int) {
		exitCode = code
	}

	main()
	if exitCode != 1 {
		t.Fatalf("expected failure exit, got %d", exitCode)
	}
}

type failingWriter struct{}

func (writer failingWriter) Write(bytes []byte) (int, error) {
	return 0, io.ErrClosedPipe
}
