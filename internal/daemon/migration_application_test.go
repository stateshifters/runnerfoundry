package daemon

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
)

func TestNewMigrationApplicationReportsInvalidPostgreSQLConfiguration(t *testing.T) {
	configPath := t.TempDir() + "/config.yaml"
	application := NewMigrationApplication()

	_, err := application.Status(t.Context(), configPath)
	if err == nil || !strings.Contains(err.Error(), "read migration config") {
		t.Fatalf("migration status error = %v", err)
	}
}

func TestMigrationApplicationUsesOnlyPostgreSQLConfiguration(t *testing.T) {
	executor := &migrationExecutorFake{status: MigrationStatus{Current: 1, Target: 2}}
	application := MigrationApplication{dependencies: migrationApplicationDependencies{
		readConfigFile: func(path string) ([]byte, error) {
			if path != "config.yaml" {
				t.Fatalf("config path = %q", path)
			}
			return []byte("github: invalid\nstate:\n  postgres:\n    role: runnerfoundry\nscale_sets: invalid\n"), nil
		},
		newExecutor: func(settings postgresconfig.Settings) (migrationExecutor, error) {
			if settings.Role != "runnerfoundry" {
				t.Fatalf("PostgreSQL settings = %#v", settings)
			}
			return migrationExecutor{backend: executor}, nil
		},
		currentTime: time.Now,
	}}

	status, err := application.Status(t.Context(), "config.yaml")
	if err != nil || status != executor.status {
		t.Fatalf("migration status = (%#v, %v)", status, err)
	}
}

func TestPostgreSQLMigrationCatalogComposesAllOwnedVersions(t *testing.T) {
	catalog, err := newPostgreSQLMigrationCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Target() != 8 {
		t.Fatalf("PostgreSQL migration target = %d, want 8", catalog.Target())
	}
}

func TestProductionMigrationExecutorConstructsWithoutConnecting(t *testing.T) {
	executor, err := newProductionMigrationExecutor(postgresconfig.Settings{Role: "runnerfoundry"})
	if err != nil || executor.backend == nil {
		t.Fatalf("production migration executor = (%v, %v)", executor, err)
	}
}

func TestProductionMigrationExecutorRejectsInvalidConnectionSettings(t *testing.T) {
	_, err := newProductionMigrationExecutor(postgresconfig.Settings{})
	if err == nil || !strings.Contains(err.Error(), "create PostgreSQL connections") {
		t.Fatalf("production migration executor error = %v", err)
	}
}

func TestPostgreSQLMigrationCatalogRejectsInvalidOwnerContributions(t *testing.T) {
	validFirst := fstest.MapFS{"001_test.sql": migrationTestFile("up", "down")}
	validThird := fstest.MapFS{"003_test.sql": migrationTestFile("up", "down")}

	tests := map[string]struct {
		neutral fs.FS
		github  fs.FS
		wantErr string
	}{
		"neutral owner": {neutral: nil, github: validFirst, wantErr: "register neutral PostgreSQL migrations"},
		"GitHub owner":  {neutral: validFirst, github: nil, wantErr: "register GitHub PostgreSQL migrations"},
		"global gap":    {neutral: validFirst, github: validThird, wantErr: "compose PostgreSQL migration catalog"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := composePostgreSQLMigrationCatalog(test.neutral, test.github)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("catalog error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestMigrationApplicationAppliesUpAndReturnsCurrentStatus(t *testing.T) {
	executor := &migrationExecutorFake{status: MigrationStatus{Current: 8, Target: 8}}
	application := testMigrationApplication(executor, time.Unix(100, 0))

	status, err := application.Up(t.Context(), "config.yaml")
	if err != nil || status != executor.status {
		t.Fatalf("migration up = (%#v, %v)", status, err)
	}
	if executor.upCalls != 1 || executor.statusCalls != 1 {
		t.Fatalf("migration calls = %#v", executor)
	}
}

func TestMigrationApplicationRollsBackUsingDaemonMemberCutoff(t *testing.T) {
	now := time.Unix(100, 0)
	executor := &migrationExecutorFake{status: MigrationStatus{Current: 3, Target: 8}}
	application := testMigrationApplication(executor, now)

	status, err := application.RollbackTo(t.Context(), "config.yaml", 3)
	if err != nil || status != executor.status {
		t.Fatalf("migration rollback = (%#v, %v)", status, err)
	}
	if executor.rollbackTarget != 3 || !executor.liveCutoff.Equal(now.Add(-runtimeMemberTTL)) {
		t.Fatalf("rollback = target %d cutoff %s", executor.rollbackTarget, executor.liveCutoff)
	}
}

func TestMigrationApplicationReportsConfigurationFailures(t *testing.T) {
	failure := errors.New("failed")
	t.Run("read config", func(t *testing.T) {
		application := MigrationApplication{dependencies: migrationApplicationDependencies{
			readConfigFile: func(string) ([]byte, error) { return nil, failure },
		}}
		if _, err := application.Status(t.Context(), "config.yaml"); !errors.Is(err, failure) {
			t.Fatalf("read error = %v", err)
		}
	})
	t.Run("load config", func(t *testing.T) {
		application := MigrationApplication{dependencies: migrationApplicationDependencies{
			readConfigFile: func(string) ([]byte, error) { return []byte("state: ["), nil },
		}}
		if _, err := application.Status(t.Context(), "config.yaml"); err == nil {
			t.Fatal("expected config error")
		}
	})
	t.Run("construct executor", func(t *testing.T) {
		application := MigrationApplication{dependencies: migrationApplicationDependencies{
			readConfigFile: func(string) ([]byte, error) { return []byte(testMigrationConfig), nil },
			newExecutor: func(postgresconfig.Settings) (migrationExecutor, error) {
				return migrationExecutor{}, failure
			},
		}}
		if _, err := application.Status(t.Context(), "config.yaml"); !errors.Is(err, failure) {
			t.Fatalf("constructor error = %v", err)
		}
	})
}

func TestMigrationMutationsStopWhenConfigurationCannotBeLoaded(t *testing.T) {
	failure := errors.New("failed")
	application := MigrationApplication{dependencies: migrationApplicationDependencies{
		readConfigFile: func(string) ([]byte, error) { return nil, failure },
	}}

	if _, err := application.Up(t.Context(), "config.yaml"); !errors.Is(err, failure) {
		t.Fatalf("migration up error = %v", err)
	}
	if _, err := application.RollbackTo(t.Context(), "config.yaml", 1); !errors.Is(err, failure) {
		t.Fatalf("migration rollback error = %v", err)
	}
}

func TestMigrationApplicationReportsExecutionFailures(t *testing.T) {
	failure := errors.New("failed")
	t.Run("status", func(t *testing.T) {
		executor := &migrationExecutorFake{statusErr: failure}
		application := testMigrationApplication(executor, time.Unix(100, 0))
		_, err := application.Status(t.Context(), "config.yaml")
		assertMigrationFailure(t, err, failure)
	})
	t.Run("up", func(t *testing.T) {
		executor := &migrationExecutorFake{upErr: failure}
		application := testMigrationApplication(executor, time.Unix(100, 0))
		_, err := application.Up(t.Context(), "config.yaml")
		assertMigrationFailure(t, err, failure)
	})
	t.Run("rollback", func(t *testing.T) {
		executor := &migrationExecutorFake{rollbackErr: failure}
		application := testMigrationApplication(executor, time.Unix(100, 0))
		_, err := application.RollbackTo(t.Context(), "config.yaml", 1)
		assertMigrationFailure(t, err, failure)
	})
}

func TestMigrationApplicationReportsStatusFailureAfterMutation(t *testing.T) {
	failure := errors.New("failed")
	for name, invoke := range map[string]func(MigrationApplication) error{
		"up": func(application MigrationApplication) error {
			_, err := application.Up(t.Context(), "config.yaml")
			return err
		},
		"rollback": func(application MigrationApplication) error {
			_, err := application.RollbackTo(t.Context(), "config.yaml", 1)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			executor := &migrationExecutorFake{statusErr: failure}
			if err := invoke(testMigrationApplication(executor, time.Unix(100, 0))); !errors.Is(err, failure) {
				t.Fatalf("status error = %v", err)
			}
		})
	}
}

const testMigrationConfig = "state:\n  postgres:\n    role: runnerfoundry\n"

func migrationTestFile(upSQL string, downSQL string) *fstest.MapFile {
	return &fstest.MapFile{Data: []byte(upSQL + "\n---- create above / drop below ----\n" + downSQL + "\n")}
}

func assertMigrationFailure(t *testing.T, err error, failure error) {
	t.Helper()
	if !errors.Is(err, failure) {
		t.Fatalf("migration error = %v", err)
	}
}

func testMigrationApplication(backend migrationBackend, now time.Time) MigrationApplication {
	return MigrationApplication{dependencies: migrationApplicationDependencies{
		readConfigFile: func(string) ([]byte, error) { return []byte(testMigrationConfig), nil },
		newExecutor: func(postgresconfig.Settings) (migrationExecutor, error) {
			return migrationExecutor{backend: backend}, nil
		},
		currentTime: func() time.Time { return now },
	}}
}

type migrationExecutorFake struct {
	status         MigrationStatus
	statusErr      error
	upErr          error
	rollbackErr    error
	statusCalls    int
	upCalls        int
	rollbackTarget int32
	liveCutoff     time.Time
}

func (executor *migrationExecutorFake) Status(context.Context) (MigrationStatus, error) {
	executor.statusCalls++
	return executor.status, executor.statusErr
}

func (executor *migrationExecutorFake) Up(context.Context) error {
	executor.upCalls++
	return executor.upErr
}

func (executor *migrationExecutorFake) RollbackTo(_ context.Context, target int32, liveCutoff time.Time) error {
	executor.rollbackTarget = target
	executor.liveCutoff = liveCutoff
	return executor.rollbackErr
}
