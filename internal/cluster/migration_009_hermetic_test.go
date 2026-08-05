package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigrationNineFreshInstall(t *testing.T) {
	model := &lifecycleBridgeMigrationFake{}
	mustMigrateLifecycleBridgeModel(t, model)
	if model.version != 9 || model.legacyPresent {
		t.Fatalf("fresh schema state = %#v", model)
	}
}

func TestMigrationNineCleanUpgradeDowngradeAndReupgradePreservesGenericHistory(t *testing.T) {
	generic := []genericLifecycleMigrationRecord{
		{key: "node/libvirt-running", provider: "libvirt", requestID: 41, phase: "busy", history: "10:20:30"},
		{key: "node/libvirt-reap", provider: "libvirt", requestID: 0, phase: "reap_pending", history: "11:nil:31"},
		{key: "node/docker", provider: "docker", requestID: 42, phase: "waiting_for_job", history: "12:22:32"},
	}
	model := &lifecycleBridgeMigrationFake{
		version: 8, legacyPresent: true,
		generic: append([]genericLifecycleMigrationRecord(nil), generic...),
		legacy: []legacyLifecycleMigrationRecord{
			{key: "node/libvirt-running", requestID: 41},
			{key: "node/libvirt-reap", requestID: 0, reapRequested: true},
		},
	}
	mustMigrateLifecycleBridgeModel(t, model)
	mustRollBackLifecycleBridgeModel(t, model)
	if !model.legacyPresent || len(model.legacy) != 2 {
		t.Fatalf("downgraded legacy state = %#v", model.legacy)
	}
	mustMigrateLifecycleBridgeModel(t, model)
	if model.version != 9 || model.legacyPresent || !slices.Equal(model.generic, generic) {
		t.Fatalf("round-trip state = %#v", model)
	}
}

func TestMigrationNineRefusesLegacyOnlyAndConflictingRowsBeforeMutation(t *testing.T) {
	model := &lifecycleBridgeMigrationFake{
		version: 8, legacyPresent: true,
		generic: []genericLifecycleMigrationRecord{
			{key: "node/conflict", provider: "libvirt", requestID: 50, phase: "busy", history: "retained"},
		},
		legacy: []legacyLifecycleMigrationRecord{
			{key: "node/legacy-only", requestID: 1},
			{key: "node/conflict", requestID: 51},
		},
	}
	err := storeWithMigrationExecutor(&fakeDatabaseConnection{}, model.executor(), nil).Migrate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "legacy_only=1, conflicting=1") {
		t.Fatalf("migration refusal = %v", err)
	}
	if model.version != 8 || !model.legacyPresent || len(model.legacy) != 2 || model.generic[0].history != "retained" {
		t.Fatalf("refused migration mutated state: %#v", model)
	}
}

type genericLifecycleMigrationRecord struct {
	key       string
	provider  string
	requestID int64
	phase     string
	history   string
}

type legacyLifecycleMigrationRecord struct {
	key           string
	requestID     int64
	reapRequested bool
}

type lifecycleBridgeMigrationFake struct {
	version       int32
	legacyPresent bool
	generic       []genericLifecycleMigrationRecord
	legacy        []legacyLifecycleMigrationRecord
}

func (model *lifecycleBridgeMigrationFake) executor() *migrationExecutor {
	return &migrationExecutor{
		currentVersion: func(context.Context) (int32, error) { return model.version, nil },
		migrateTo: func(_ context.Context, target int32) error {
			switch target {
			case 9:
				legacyOnly, conflicting := model.preflightCounts()
				if legacyOnly+conflicting > 0 {
					return &pgconn.PgError{Code: "P0001", Message: fmt.Sprintf(
						"cannot retire legacy libvirt lifecycle: legacy_only=%d, conflicting=%d",
						legacyOnly, conflicting)}
				}
				model.legacy = nil
				model.legacyPresent = false
			case 8:
				model.reconstructLegacy()
			default:
				return errors.New("unsupported fake migration target")
			}
			model.version = target
			return nil
		},
		target: 9,
	}
}

func (model *lifecycleBridgeMigrationFake) preflightCounts() (legacyOnly int, conflicting int) {
	genericByKey := make(map[string]genericLifecycleMigrationRecord, len(model.generic))
	for _, lifecycle := range model.generic {
		genericByKey[lifecycle.key] = lifecycle
	}
	for _, legacy := range model.legacy {
		generic, present := genericByKey[legacy.key]
		if !present {
			legacyOnly++
			continue
		}
		if generic.provider != "libvirt" || generic.requestID != legacy.requestID ||
			(generic.phase == "reap_pending") != legacy.reapRequested {
			conflicting++
		}
	}
	return legacyOnly, conflicting
}

func (model *lifecycleBridgeMigrationFake) reconstructLegacy() {
	model.legacy = nil
	for _, generic := range model.generic {
		if generic.provider == "libvirt" {
			model.legacy = append(model.legacy, legacyLifecycleMigrationRecord{
				key: generic.key, requestID: generic.requestID, reapRequested: generic.phase == "reap_pending",
			})
		}
	}
	model.legacyPresent = true
}

func mustMigrateLifecycleBridgeModel(t *testing.T, model *lifecycleBridgeMigrationFake) {
	t.Helper()
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{int32Row(t, 9)}}
	if err := storeWithMigrationExecutor(connection, model.executor(), nil).Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func mustRollBackLifecycleBridgeModel(t *testing.T, model *lifecycleBridgeMigrationFake) {
	t.Helper()
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, false)}}
	if err := storeWithMigrationExecutor(connection, model.executor(), nil).
		MigrateDown(context.Background(), 8, time.Unix(100, 0), time.Minute); err != nil {
		t.Fatal(err)
	}
}
