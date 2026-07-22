//go:build integration

package cluster

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
)

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
		if removeErr := os.RemoveAll(server.runtimePath); removeErr != nil {
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
