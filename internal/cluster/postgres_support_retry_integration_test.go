//go:build integration

package cluster

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLRuntimeRemovalRetriesTransientExecutableLock(t *testing.T) {
	attempts := 0
	waits := 0
	err := removePostgreSQLRuntimeWithRetry("runtime", func(path string) error {
		if path != "runtime" {
			t.Fatalf("removal path = %q", path)
		}
		attempts++
		if attempts < 3 {
			return errors.New("executable still locked")
		}
		return nil
	}, func(delay time.Duration) {
		if delay != postgreSQLRuntimeRemovalRetryDelay {
			t.Fatalf("retry delay = %s", delay)
		}
		waits++
	})
	if err != nil || attempts != 3 || waits != 2 {
		t.Fatalf("removal error=%v attempts=%d waits=%d", err, attempts, waits)
	}
}

func TestPostgreSQLRuntimeRemovalReportsPersistentFailure(t *testing.T) {
	attempts := 0
	err := removePostgreSQLRuntimeWithRetry("runtime", func(string) error {
		attempts++
		return errors.New("persistent lock")
	}, func(time.Duration) {})
	if err == nil || attempts != postgreSQLRuntimeRemovalAttemptLimit {
		t.Fatalf("removal error=%v attempts=%d", err, attempts)
	}
}

func TestPostgreSQLIntegrationServerRetriesPortCollisions(t *testing.T) {
	occupiedPort := postgreSQLIntegrationPort(t)
	attempts := 0

	server, err := startPostgreSQLIntegrationServerWithPortCandidate(func() (uint32, error) {
		attempts++
		if attempts == 1 {
			return occupiedPort, nil
		}
		return randomPostgreSQLPort()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if stopErr := server.database.Stop(); stopErr != nil {
			t.Errorf("stop retry-test PostgreSQL: %v", stopErr)
		}
		if removeErr := removePostgreSQLRuntime(server.runtimePath); removeErr != nil {
			t.Errorf("remove retry-test PostgreSQL runtime: %v", removeErr)
		}
	})

	if attempts < 2 {
		t.Fatalf("PostgreSQL startup attempts = %d, want at least 2", attempts)
	}
	connection, err := pgx.Connect(context.Background(), server.dsn)
	if err != nil {
		t.Fatalf("connect to retry-test PostgreSQL: %v", err)
	}
	if err = connection.Close(context.Background()); err != nil {
		t.Fatalf("close retry-test PostgreSQL connection: %v", err)
	}
}

func postgreSQLIntegrationPort(t *testing.T) uint32 {
	t.Helper()
	parsedDSN, err := url.Parse(postgreSQLIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(parsedDSN.Port(), 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	return uint32(port)
}
