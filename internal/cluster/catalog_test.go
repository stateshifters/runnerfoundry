package cluster

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

func TestCatalogComposesOwnerFragmentsInDependencyOrder(t *testing.T) {
	neutral := migrationSet(t, "state", 100, fstest.MapFS{
		"001_neutral.sql": migrationFile("neutral up", "neutral down"),
		"002_neutral.sql": migrationFile("second up", "second down"),
	})
	source := migrationSet(t, "github", 200, fstest.MapFS{
		"001_github.sql": migrationFile("github up", "github down"),
	})

	catalog, err := NewCatalog(source, neutral)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Target() != 2 {
		t.Fatalf("target = %d, want 2", catalog.Target())
	}
	first := catalog.migrations[0]
	if strings.Index(first.upSQL, "neutral up") > strings.Index(first.upSQL, "github up") {
		t.Fatalf("up SQL is not owner ordered: %q", first.upSQL)
	}
	if strings.Index(first.downSQL, "github down") > strings.Index(first.downSQL, "neutral down") {
		t.Fatalf("down SQL is not reverse owner ordered: %q", first.downSQL)
	}
}

func TestCatalogRejectsInvalidOwnerCatalogs(t *testing.T) {
	tests := map[string][]MigrationSet{
		"missing owner": {{order: 100, files: validMigrationFiles(1)}},
		"invalid order": {{owner: "state", files: validMigrationFiles(1)}},
		"missing files": {{owner: "state", order: 100}},
		"duplicate owner": {
			migrationSet(t, "state", 100, validMigrationFiles(1)),
			migrationSet(t, "state", 200, validMigrationFiles(2)),
		},
		"duplicate order at sequence": {
			migrationSet(t, "state", 100, validMigrationFiles(1)),
			migrationSet(t, "github", 100, validMigrationFiles(1)),
		},
		"sequence gap": {migrationSet(t, "state", 100, validMigrationFiles(2))},
	}
	assertInvalidCatalogs(t, tests)
}

func TestCatalogRejectsInvalidMigrationFiles(t *testing.T) {
	tests := map[string][]MigrationSet{
		"duplicate owner sequence": {migrationSet(t, "state", 100, fstest.MapFS{
			"001_one.sql": migrationFile("up", "down"),
			"001_two.sql": migrationFile("up", "down"),
		})},
		"unknown file": {migrationSet(t, "state", 100, fstest.MapFS{
			"readme.txt": {Data: []byte("unknown")},
		})},
		"zero sequence": {migrationSet(t, "state", 100, fstest.MapFS{
			"000_invalid.sql": migrationFile("up", "down"),
		})},
		"missing up": {migrationSet(t, "state", 100, fstest.MapFS{
			"001_empty.sql": migrationFile("", "down"),
		})},
		"missing down": {migrationSet(t, "state", 100, fstest.MapFS{
			"001_empty.sql": migrationFile("up", ""),
		})},
	}
	assertInvalidCatalogs(t, tests)
}

func assertInvalidCatalogs(t *testing.T, tests map[string][]MigrationSet) {
	t.Helper()
	for name, sets := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewCatalog(sets...); err == nil {
				t.Fatal("invalid catalog was accepted")
			}
		})
	}
}

func TestMigrationSetConstructorRejectsInvalidIdentityAndFiles(t *testing.T) {
	if _, err := NewMigrationSet("", 100, validMigrationFiles(1)); err == nil {
		t.Fatal("missing owner was accepted")
	}
	if _, err := NewMigrationSet("state", 0, validMigrationFiles(1)); err == nil {
		t.Fatal("non-positive order was accepted")
	}
	if _, err := NewMigrationSet("state", 100, nil); err == nil {
		t.Fatal("missing files were accepted")
	}
}

func TestCatalogHandlesEmptyAndUnreadableOwnerCatalogs(t *testing.T) {
	empty, err := NewCatalog()
	if err != nil || empty.Target() != 0 || (*Catalog)(nil).Target() != 0 {
		t.Fatalf("empty catalog = %#v, %v", empty, err)
	}
	set, err := NewMigrationSet("state", 100, failingMigrationFS{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewCatalog(set); err == nil {
		t.Fatal("filesystem failure was hidden")
	}
	set = migrationSet(t, "state", 100, readFailMigrationFS{base: validMigrationFiles(1)})
	if _, err = NewCatalog(set); err == nil {
		t.Fatal("migration read failure was hidden")
	}
}

func TestCatalogRejectsMalformedDirectionSeparator(t *testing.T) {
	for name, contents := range map[string]string{
		"missing":  "SELECT 1",
		"repeated": "up\n" + migrationDirectionSeparator + "\ndown\n" + migrationDirectionSeparator + "\nagain",
	} {
		t.Run(name, func(t *testing.T) {
			set := migrationSet(t, "state", 100, fstest.MapFS{"001_test.sql": {Data: []byte(contents)}})
			if _, err := NewCatalog(set); err == nil {
				t.Fatal("malformed migration was accepted")
			}
		})
	}
}

func migrationSet(t *testing.T, owner string, order int, files fs.FS) MigrationSet {
	t.Helper()
	set, err := NewMigrationSet(owner, order, files)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func validMigrationFiles(sequence int) fstest.MapFS {
	name := fmt.Sprintf("%03d_test.sql", sequence)
	return fstest.MapFS{name: migrationFile("up", "down")}
}

func migrationFile(upSQL string, downSQL string) *fstest.MapFile {
	return &fstest.MapFile{Data: []byte(upSQL + "\n---- create above / drop below ----\n" + downSQL + "\n")}
}

type failingMigrationFS struct{}

func (failingMigrationFS) Open(string) (fs.File, error) {
	return nil, errors.New("failed")
}

type readFailMigrationFS struct {
	base fstest.MapFS
}

func (files readFailMigrationFS) Open(name string) (fs.File, error) {
	if name == "001_test.sql" {
		return nil, errors.New("failed")
	}
	file, err := files.base.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open base migration file: %w", err)
	}
	return file, nil
}
