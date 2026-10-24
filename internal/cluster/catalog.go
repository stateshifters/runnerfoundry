package cluster

import (
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/tern/v2/migrate"
)

const migrationDirectionSeparator = "---- create above / drop below ----"

type MigrationSet struct {
	owner string
	order int
	files fs.FS
}

func NewMigrationSet(owner string, order int, files fs.FS) (MigrationSet, error) {
	if strings.TrimSpace(owner) == "" {
		return MigrationSet{}, errors.New("PostgreSQL migration owner is required")
	}
	if order <= 0 {
		return MigrationSet{}, errors.New("PostgreSQL migration owner order must be positive")
	}
	if files == nil {
		return MigrationSet{}, errors.New("PostgreSQL migration files are required")
	}
	return MigrationSet{owner: owner, order: order, files: files}, nil
}

type Catalog struct {
	migrations []catalogMigration
	target     int32
}

type catalogMigration struct {
	sequence int32
	name     string
	upSQL    string
	downSQL  string
}

type migrationFragment struct {
	owner    string
	order    int
	sequence int32
	name     string
	upSQL    string
	downSQL  string
}

func NewCatalog(sets ...MigrationSet) (*Catalog, error) {
	fragments, err := loadMigrationFragments(sets)
	if err != nil {
		return nil, err
	}
	return composeMigrationCatalog(fragments)
}

func (catalog *Catalog) Target() int32 {
	if catalog == nil {
		return 0
	}
	return catalog.target
}

func (catalog *Catalog) appendTo(migrator *migrate.Migrator) {
	for _, migration := range catalog.migrations {
		migrator.AppendMigration(migration.name, migration.upSQL, migration.downSQL)
	}
}

func loadMigrationFragments(sets []MigrationSet) ([]migrationFragment, error) {
	owners := make(map[string]struct{}, len(sets))
	fragments := make([]migrationFragment, 0)
	for _, set := range sets {
		if err := validateMigrationSet(set, owners); err != nil {
			return nil, err
		}
		setFragments, err := loadMigrationSet(set)
		if err != nil {
			return nil, fmt.Errorf("load PostgreSQL migrations for %s: %w", set.owner, err)
		}
		fragments = append(fragments, setFragments...)
	}
	return fragments, nil
}

func validateMigrationSet(set MigrationSet, owners map[string]struct{}) error {
	if strings.TrimSpace(set.owner) == "" || set.order <= 0 || set.files == nil {
		return errors.New("invalid PostgreSQL migration set")
	}
	if _, exists := owners[set.owner]; exists {
		return fmt.Errorf("duplicate PostgreSQL migration owner %q", set.owner)
	}
	owners[set.owner] = struct{}{}
	return nil
}

func loadMigrationSet(set MigrationSet) ([]migrationFragment, error) {
	fragments := make([]migrationFragment, 0)
	seenSequences := make(map[int32]struct{})
	err := fs.WalkDir(set.files, ".", func(filename string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk PostgreSQL migration file %q: %w", filename, walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		fragment, fragmentErr := readMigrationFragment(set, filename)
		if fragmentErr != nil {
			return fragmentErr
		}
		return appendMigrationFragment(&fragments, seenSequences, fragment)
	})
	if err != nil {
		return nil, fmt.Errorf("walk PostgreSQL migration files: %w", err)
	}
	return fragments, nil
}

func readMigrationFragment(set MigrationSet, filename string) (migrationFragment, error) {
	matches := regexp.MustCompile(`^(\d{3})_([a-z0-9][a-z0-9_]*)\.sql$`).FindStringSubmatch(filename)
	if matches == nil {
		return migrationFragment{}, fmt.Errorf("unknown PostgreSQL migration file %q", filename)
	}
	sequence := int32(matches[1][0]-'0')*100 + int32(matches[1][1]-'0')*10 + int32(matches[1][2]-'0')
	if sequence == 0 {
		return migrationFragment{}, fmt.Errorf("invalid PostgreSQL migration sequence in %q", filename)
	}
	contents, err := fs.ReadFile(set.files, filename)
	if err != nil {
		return migrationFragment{}, fmt.Errorf("read PostgreSQL migration %q: %w", filename, err)
	}
	upSQL, downSQL, err := splitMigrationSQL(string(contents))
	if err != nil {
		return migrationFragment{}, fmt.Errorf("parse %s: %w", filename, err)
	}
	return migrationFragment{
		owner: set.owner, order: set.order, sequence: sequence,
		name: matches[2], upSQL: upSQL, downSQL: downSQL,
	}, nil
}

func appendMigrationFragment(
	fragments *[]migrationFragment,
	seenSequences map[int32]struct{},
	fragment migrationFragment,
) error {
	if _, exists := seenSequences[fragment.sequence]; exists {
		return fmt.Errorf("duplicate PostgreSQL migration sequence %03d for %s", fragment.sequence, fragment.owner)
	}
	seenSequences[fragment.sequence] = struct{}{}
	*fragments = append(*fragments, fragment)
	return nil
}

func splitMigrationSQL(contents string) (upSQL string, downSQL string, err error) {
	parts := strings.Split(contents, migrationDirectionSeparator)
	if len(parts) != 2 {
		return "", "", errors.New("PostgreSQL migration must contain one direction separator")
	}
	upSQL = strings.TrimSpace(parts[0])
	downSQL = strings.TrimSpace(parts[1])
	if upSQL == "" {
		return "", "", errors.New("PostgreSQL migration is missing Up SQL")
	}
	if downSQL == "" {
		return "", "", errors.New("PostgreSQL migration is missing Down SQL")
	}
	return upSQL, downSQL, nil
}

func composeMigrationCatalog(fragments []migrationFragment) (*Catalog, error) {
	if len(fragments) == 0 {
		return &Catalog{}, nil
	}
	sort.Slice(fragments, func(left int, right int) bool {
		if fragments[left].sequence != fragments[right].sequence {
			return fragments[left].sequence < fragments[right].sequence
		}
		return fragments[left].order < fragments[right].order
	})
	grouped := make(map[int32][]migrationFragment)
	for _, fragment := range fragments {
		sequenceFragments := grouped[fragment.sequence]
		if len(sequenceFragments) > 0 && sequenceFragments[len(sequenceFragments)-1].order == fragment.order {
			return nil, fmt.Errorf("duplicate PostgreSQL migration order %d at sequence %03d", fragment.order, fragment.sequence)
		}
		grouped[fragment.sequence] = append(sequenceFragments, fragment)
	}
	target := fragments[len(fragments)-1].sequence
	catalog := &Catalog{migrations: make([]catalogMigration, 0, target), target: target}
	for sequence := int32(1); sequence <= target; sequence++ {
		sequenceFragments := grouped[sequence]
		if len(sequenceFragments) == 0 {
			return nil, fmt.Errorf("missing PostgreSQL migration sequence %03d", sequence)
		}
		catalog.migrations = append(catalog.migrations, composeMigration(sequence, sequenceFragments))
	}
	return catalog, nil
}

func composeMigration(sequence int32, fragments []migrationFragment) catalogMigration {
	upParts := make([]string, 0, len(fragments))
	downParts := make([]string, 0, len(fragments))
	names := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		upParts = append(upParts, fragment.upSQL)
		names = append(names, fragment.owner+"_"+fragment.name)
	}
	for index := len(fragments) - 1; index >= 0; index-- {
		downParts = append(downParts, fragments[index].downSQL)
	}
	return catalogMigration{
		sequence: sequence,
		name:     strings.Join(names, "__"),
		upSQL:    strings.Join(upParts, "\n\n"),
		downSQL:  strings.Join(downParts, "\n\n"),
	}
}
