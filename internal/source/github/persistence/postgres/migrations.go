package postgres

import (
	"embed"
	"fmt"
	"io/fs"
	"path"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func Migrations() fs.FS { return ownerMigrations{files: migrationFiles} }

type ownerMigrations struct{ files embed.FS }

func (migrations ownerMigrations) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	file, err := migrations.files.Open(path.Join("migrations", name))
	if err != nil {
		return nil, fmt.Errorf("open GitHub PostgreSQL migration %q: %w", name, err)
	}
	return file, nil
}
