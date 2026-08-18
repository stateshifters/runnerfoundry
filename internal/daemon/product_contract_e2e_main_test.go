//go:build e2e

package daemon

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

type e2eBackendSelection string

const (
	e2eBackendSQLite   e2eBackendSelection = "sqlite"
	e2eBackendPostgres e2eBackendSelection = "postgres"
	e2eBackendAll      e2eBackendSelection = "all"
)

var selectedE2EBackend = e2eBackendSQLite

func TestMain(testSuite *testing.M) {
	selection, err := parseE2EBackendSelection(os.Getenv("E2E_BACKEND"))
	if err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(2)
		}
		os.Exit(2)
	}
	selectedE2EBackend = selection
	os.Exit(testSuite.Run())
}

func parseE2EBackendSelection(value string) (e2eBackendSelection, error) {
	switch normalized := strings.TrimSpace(strings.ToLower(value)); normalized {
	case "", string(e2eBackendSQLite):
		return e2eBackendSQLite, nil
	case string(e2eBackendPostgres):
		return e2eBackendPostgres, nil
	case string(e2eBackendAll):
		return e2eBackendAll, nil
	default:
		return "", errors.New("unsupported E2E_BACKEND: " + value + " (supported: all, sqlite, postgres)")
	}
}

func (selection e2eBackendSelection) enables(backend string) bool {
	return selection == e2eBackendAll || string(selection) == backend
}

func TestE2EBackendParsingContract(t *testing.T) {
	testCases := []struct {
		name  string
		value string
		want  e2eBackendSelection
	}{
		{name: "empty is safe SQLite", want: e2eBackendSQLite},
		{name: "whitespace is safe SQLite", value: "  ", want: e2eBackendSQLite},
		{name: "SQLite", value: "sqlite", want: e2eBackendSQLite},
		{name: "PostgreSQL", value: "postgres", want: e2eBackendPostgres},
		{name: "all", value: "all", want: e2eBackendAll},
		{name: "normalized", value: " POSTGRES ", want: e2eBackendPostgres},
	}
	for _, testCase := range testCases {
		selection, err := parseE2EBackendSelection(testCase.value)
		if err != nil || selection != testCase.want {
			t.Fatalf("%s selection = %q, error = %v, want %q", testCase.name, selection, err, testCase.want)
		}
	}
	if _, err := parseE2EBackendSelection("unknown"); err == nil {
		t.Fatal("unknown E2E backend selection was accepted")
	}
}

func TestE2EBackendSelectionContract(t *testing.T) {
	if !e2eBackendAll.enables("sqlite") || !e2eBackendAll.enables("postgres") {
		t.Fatal("all E2E backend selection did not enable both database variants")
	}
	if !e2eBackendSQLite.enables("sqlite") || e2eBackendSQLite.enables("postgres") {
		t.Fatal("SQLite E2E backend selection did not isolate SQLite scenarios")
	}
	if !e2eBackendPostgres.enables("postgres") || e2eBackendPostgres.enables("sqlite") {
		t.Fatal("PostgreSQL E2E backend selection did not isolate PostgreSQL scenarios")
	}
}
