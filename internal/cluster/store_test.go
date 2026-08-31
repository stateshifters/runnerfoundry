package cluster

import (
	"context"
	"testing"
)

func TestConnectDirectPostgreSQLValidatesSettingsBeforeResolvingRole(t *testing.T) {
	resolver := testConnectionResolver(nil)
	resolver.lookupEnvironment = func(string) (string, bool) {
		t.Fatal("role resolution occurred before structural validation")
		return "", false
	}
	store := newStoreWithResolver(Settings{
		Role:   "env://POSTGRES_ROLE",
		Direct: &Endpoint{DSN: "postgres://database/scaler", Host: "database"},
	}, resolver)

	_, err := store.connectDirectPostgreSQL(context.Background())
	expectedError := "validate PostgreSQL settings: " +
		"state.postgres.direct.dsn cannot be combined with structured PostgreSQL settings"
	if err == nil || err.Error() != expectedError {
		t.Fatalf("connectDirectPostgreSQL() error = %v, want %q", err, expectedError)
	}
}

func TestConnectDirectPostgreSQLRejectsUnresolvableRole(t *testing.T) {
	store := newStoreWithResolver(Settings{Role: "env://MISSING_ROLE"}, testConnectionResolver(nil))

	_, err := store.connectDirectPostgreSQL(context.Background())
	expectedError := "resolve PostgreSQL role: PostgreSQL environment reference is missing or empty"
	if err == nil || err.Error() != expectedError {
		t.Fatalf("connectDirectPostgreSQL() error = %v, want %q", err, expectedError)
	}
}
