package postgres

import (
	"errors"
	"io/fs"
	"reflect"
	"testing"
)

func TestMigrationsExposeOwnerLocalFiles(t *testing.T) {
	t.Parallel()
	entries, err := fs.ReadDir(Migrations(), ".")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	want := []string{
		"001_initial.sql",
		"003_runner_attempt_identity.sql",
		"004_queued_job_notifications.sql",
		"007_provisioning_failure_reservations.sql",
		"008_validate_provisioning_failure_fence.sql",
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("migrations = %#v, want %#v", names, want)
	}
}

func TestMigrationsRejectInvalidAndMissingNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want error
	}{
		{name: "../migration.sql", want: fs.ErrInvalid},
		{name: "999_missing.sql", want: fs.ErrNotExist},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := Migrations().Open(testCase.name)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("open error = %v, want %v", err, testCase.want)
			}
		})
	}
}
