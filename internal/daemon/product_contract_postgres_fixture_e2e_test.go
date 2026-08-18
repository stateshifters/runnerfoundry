//go:build e2e

package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

const (
	e2ePostgresAdminDatabase = "postgres"
	e2ePostgresAdminUser     = "postgres"
	e2ePostgresAdminPassword = "postgres"
	e2ePostgresLoginRole     = "runnerfoundry_e2e_login"
	e2ePostgresStableRole    = "runnerfoundry_e2e"
	e2ePostgresLoginPassword = "runnerfoundry_e2e_password"
	e2ePostgresStartAttempts = 3
)

type e2ePostgresFixture struct {
	database        *embeddedpostgres.EmbeddedPostgres
	adminConnection *pgx.Conn
	port            uint32
	databaseName    string
	logs            *e2ePostgresLogBuffer
	closeMutex      sync.Mutex
	closed          bool
	closeResult     error
}

type e2ePostgresLogBuffer struct {
	mutex sync.Mutex
	data  []byte
}

func (buffer *e2ePostgresLogBuffer) Write(content []byte) (int, error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	buffer.data = append(buffer.data, content...)
	return len(content), nil
}

func (buffer *e2ePostgresLogBuffer) String() string {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	return string(buffer.data)
}

func e2ePostgresStart(t *testing.T) *e2ePostgresFixture {
	t.Helper()
	database, port, logs := e2ePostgresStartProcess(t)
	fixture := &e2ePostgresFixture{
		database: database, port: port,
		databaseName: e2ePostgresDatabaseName(t.Name()), logs: logs,
	}
	e2ePostgresRegisterCleanup(t, fixture)
	fixture.e2ePostgresConnectAndProvision(t)
	return fixture
}

func e2ePostgresStartProcess(
	t *testing.T,
) (*embeddedpostgres.EmbeddedPostgres, uint32, *e2ePostgresLogBuffer) {
	t.Helper()
	binariesPath := e2ePostgresBinariesPath(t)
	version := e2eConfiguredPostgresVersion(t)
	attemptLogs := make([]string, 0, e2ePostgresStartAttempts)
	for attempt := 1; attempt <= e2ePostgresStartAttempts; attempt++ {
		port := e2ePostgresLoopbackPort(t)
		logs := &e2ePostgresLogBuffer{}
		database := embeddedpostgres.NewDatabase(e2ePostgresProcessConfiguration(
			t.TempDir(), binariesPath, version, port, logs,
		))
		err := database.Start()
		if err == nil {
			return database, port, logs
		}
		attemptLogs = append(attemptLogs, fmt.Sprintf("attempt %d on port %d: %v\n%s", attempt, port, err, logs.String()))
		if !e2ePostgresBindConflict(err, logs.String()) {
			t.Fatalf("start embedded PostgreSQL %s from %s: %v\nPostgreSQL logs:\n%s",
				version, binariesPath, err, logs.String())
		}
	}
	t.Fatalf("start embedded PostgreSQL %s from %s after %d bind conflicts:\n%s",
		version, binariesPath, e2ePostgresStartAttempts, strings.Join(attemptLogs, "\n"))
	return nil, 0, nil
}

func e2ePostgresBindConflict(startErr error, logs string) bool {
	message := strings.ToLower(startErr.Error() + "\n" + logs)
	for _, fragment := range []string{
		"address already in use",
		"could not bind",
		"could not create any tcp/ip sockets",
		"only one usage of each socket address",
	} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func e2ePostgresBinariesPath(t *testing.T) string {
	t.Helper()
	binariesPath := strings.TrimSpace(os.Getenv("E2E_POSTGRES_BINARIES"))
	if binariesPath == "" {
		t.Fatal("E2E_POSTGRES_BINARIES is required and must point to pre-extracted PostgreSQL binaries")
	}
	binariesPath, err := filepath.Abs(binariesPath)
	if err != nil {
		t.Fatalf("resolve E2E_POSTGRES_BINARIES: %v", err)
	}
	if err = e2ePostgresValidateBinariesPath(binariesPath); err != nil {
		t.Fatal(err)
	}
	return binariesPath
}

func e2eConfiguredPostgresVersion(t *testing.T) embeddedpostgres.PostgresVersion {
	t.Helper()
	version, err := e2ePostgresVersion(strings.TrimSpace(os.Getenv("E2E_POSTGRES_VERSION")))
	if err != nil {
		t.Fatal(err)
	}
	return version
}

func e2ePostgresLoopbackPort(t *testing.T) uint32 {
	t.Helper()
	port, err := e2ePostgresFreeLoopbackPort()
	if err != nil {
		t.Fatalf("reserve PostgreSQL loopback port: %v", err)
	}
	return port
}

func e2ePostgresProcessConfiguration(
	temporaryDirectory string,
	binariesPath string,
	version embeddedpostgres.PostgresVersion,
	port uint32,
	logs *e2ePostgresLogBuffer,
) embeddedpostgres.Config {
	return embeddedpostgres.DefaultConfig().
		Version(version).
		Port(port).
		Database(e2ePostgresAdminDatabase).
		Username(e2ePostgresAdminUser).
		Password(e2ePostgresAdminPassword).
		BinariesPath(binariesPath).
		RuntimePath(filepath.Join(temporaryDirectory, "runtime")).
		DataPath(filepath.Join(temporaryDirectory, "data")).
		StartTimeout(30 * time.Second).
		Logger(logs)
}

func e2ePostgresRegisterCleanup(t *testing.T, fixture *e2ePostgresFixture) {
	t.Helper()
	t.Cleanup(func() {
		if closeErr := fixture.Close(); closeErr != nil {
			t.Errorf("close embedded PostgreSQL fixture: %v", closeErr)
		}
		if t.Failed() {
			t.Logf("PostgreSQL logs:\n%s", fixture.logs.String())
		}
	})
}

func (fixture *e2ePostgresFixture) e2ePostgresConnectAndProvision(t *testing.T) {
	t.Helper()
	adminConfig, err := fixture.e2ePostgresAdminConnectionConfig()
	if err != nil {
		t.Fatalf("configure embedded PostgreSQL administrator connection: %v", err)
	}
	adminConnection, err := pgx.ConnectConfig(context.Background(), adminConfig)
	if err != nil {
		t.Fatalf("connect to embedded PostgreSQL administrator database: %v", err)
	}
	fixture.adminConnection = adminConnection
	if err = fixture.e2ePostgresProvision(context.Background()); err != nil {
		t.Fatalf("provision embedded PostgreSQL fixture: %v", err)
	}
	if err = fixture.e2ePostgresVerifyRoleIdentity(context.Background(), fixture.Settings()); err != nil {
		t.Fatalf("verify embedded PostgreSQL role identity: %v", err)
	}
}

func (fixture *e2ePostgresFixture) Settings() postgresconfig.Settings {
	return postgresconfig.Settings{
		Host: "127.0.0.1", Port: strconv.FormatUint(uint64(fixture.port), 10), Name: fixture.databaseName,
		User: e2ePostgresLoginRole, Password: e2ePostgresLoginPassword, Role: e2ePostgresStableRole,
	}
}

func (fixture *e2ePostgresFixture) terminateLeaderConnection(ctx context.Context) error {
	var terminated bool
	err := fixture.adminConnection.QueryRow(ctx, `
		SELECT pg_terminate_backend(locks.pid)
		FROM pg_locks AS locks
		JOIN pg_database AS databases ON databases.oid = locks.database
		WHERE locks.locktype = 'advisory' AND locks.granted AND databases.datname = $1
		LIMIT 1`, fixture.databaseName).Scan(&terminated)
	if err != nil {
		return fmt.Errorf("terminate PostgreSQL leader connection: %w", err)
	}
	if !terminated {
		return errors.New("PostgreSQL leader connection was not terminated")
	}
	return nil
}

func (fixture *e2ePostgresFixture) Close() error {
	fixture.closeMutex.Lock()
	defer fixture.closeMutex.Unlock()
	if fixture.closed {
		return fixture.closeResult
	}
	fixture.closed = true

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var result error
	if fixture.adminConnection != nil {
		result = errors.Join(result, fixture.e2ePostgresDropProvisionedResources(ctx))
		result = errors.Join(result, fixture.adminConnection.Close(ctx))
	}
	if fixture.database != nil {
		result = errors.Join(result, fixture.database.Stop())
	}
	fixture.closeResult = result

	return result
}

func (fixture *e2ePostgresFixture) e2ePostgresProvision(ctx context.Context) error {
	stableRole := pgx.Identifier{e2ePostgresStableRole}.Sanitize()
	loginRole := pgx.Identifier{e2ePostgresLoginRole}.Sanitize()
	databaseName := pgx.Identifier{fixture.databaseName}.Sanitize()
	statements := []string{
		"CREATE ROLE " + stableRole + " NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION",
		"CREATE ROLE " + loginRole + " LOGIN PASSWORD '" + e2ePostgresLoginPassword + "' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION",
		"GRANT " + stableRole + " TO " + loginRole,
		"CREATE DATABASE " + databaseName + " OWNER " + stableRole,
	}
	for _, statement := range statements {
		if _, err := fixture.adminConnection.Exec(ctx, statement); err != nil {
			return fmt.Errorf("execute PostgreSQL fixture administration statement: %w", err)
		}
	}

	return nil
}

func (fixture *e2ePostgresFixture) e2ePostgresDropProvisionedResources(ctx context.Context) error {
	stableRole := pgx.Identifier{e2ePostgresStableRole}.Sanitize()
	loginRole := pgx.Identifier{e2ePostgresLoginRole}.Sanitize()
	databaseName := pgx.Identifier{fixture.databaseName}.Sanitize()
	statements := []string{
		"DROP DATABASE " + databaseName,
		"DROP ROLE " + loginRole,
		"DROP ROLE " + stableRole,
	}
	var result error
	for _, statement := range statements {
		if _, err := fixture.adminConnection.Exec(ctx, statement); err != nil {
			result = errors.Join(result, fmt.Errorf("execute PostgreSQL fixture cleanup statement: %w", err))
		}
	}

	return result
}

func (fixture *e2ePostgresFixture) e2ePostgresVerifyRoleIdentity(
	ctx context.Context,
	settings postgresconfig.Settings,
) error {
	connection, err := fixture.e2ePostgresConnectAsApplication(ctx, settings)
	if err != nil {
		return err
	}
	identityErr := e2ePostgresAssertRoleIdentity(ctx, connection)
	closeErr := connection.Close(context.WithoutCancel(ctx))
	if closeErr != nil {
		closeErr = fmt.Errorf("close PostgreSQL role verification connection: %w", closeErr)
	}
	return errors.Join(identityErr, closeErr)
}

func (fixture *e2ePostgresFixture) e2ePostgresConnectAsApplication(
	ctx context.Context,
	settings postgresconfig.Settings,
) (*pgx.Conn, error) {
	connectionConfig, err := pgx.ParseConfig(fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		settings.Host, settings.Port, settings.Name, settings.User, settings.Password,
	))
	if err != nil {
		return nil, fmt.Errorf("prepare PostgreSQL role verification connection: %w", err)
	}
	connection, err := pgx.ConnectConfig(ctx, connectionConfig)
	if err != nil {
		return nil, fmt.Errorf("connect as PostgreSQL fixture login: %w", err)
	}
	if _, err = connection.Exec(ctx, "SET ROLE "+pgx.Identifier{settings.Role}.Sanitize()); err != nil {
		closeErr := connection.Close(context.WithoutCancel(ctx))
		return nil, errors.Join(fmt.Errorf("assume PostgreSQL fixture stable role: %w", err), closeErr)
	}
	return connection, nil
}

func e2ePostgresAssertRoleIdentity(
	ctx context.Context,
	connection *pgx.Conn,
) error {
	var sessionUser string
	var currentUser string
	if err := connection.QueryRow(ctx, "SELECT session_user, current_user").Scan(&sessionUser, &currentUser); err != nil {
		return fmt.Errorf("read PostgreSQL fixture role identity: %w", err)
	}
	if sessionUser != e2ePostgresLoginRole || currentUser != e2ePostgresStableRole {
		return fmt.Errorf("unexpected PostgreSQL role identity: session_user=%q current_user=%q", sessionUser, currentUser)
	}
	return nil
}

func (fixture *e2ePostgresFixture) e2ePostgresAssertStableRoleOwnership(ctx context.Context) (resultErr error) {
	settings := fixture.Settings()
	connection, err := fixture.e2ePostgresConnectAsApplication(ctx, settings)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := connection.Close(context.WithoutCancel(ctx)); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close PostgreSQL ownership connection: %w", closeErr))
		}
	}()

	inventory, inventoryErr := e2ePostgresReadOwnershipInventory(ctx, connection, settings.Role)
	roleErr := e2ePostgresValidateStableRole(ctx, connection, settings.Role)
	if inventoryErr != nil {
		return errors.Join(inventoryErr, roleErr)
	}
	return errors.Join(inventory.validate(), roleErr)
}

type e2ePostgresOwnershipInventory struct {
	objectCounts      map[string]int
	versionTableFound bool
	ownershipErrors   []error
}

const e2ePostgresOwnershipSQL = `
	SELECT CASE objects.relkind
		WHEN 'r' THEN 'table'
		WHEN 'p' THEN 'partitioned table'
		WHEN 'S' THEN 'sequence'
		WHEN 'i' THEN 'index'
	END,
		namespaces.nspname || '.' || objects.relname,
		pg_get_userbyid(objects.relowner)
	FROM pg_catalog.pg_class AS objects
	JOIN pg_catalog.pg_namespace AS namespaces ON namespaces.oid = objects.relnamespace
	WHERE namespaces.nspname = 'public'
		AND objects.relname LIKE 'runnerfoundry\_%' ESCAPE '\'
		AND objects.relkind IN ('r', 'p', 'S', 'i')
	UNION ALL
	SELECT 'function',
		namespaces.nspname || '.' || routines.proname || '(' ||
			pg_get_function_identity_arguments(routines.oid) || ')',
		pg_get_userbyid(routines.proowner)
	FROM pg_catalog.pg_proc AS routines
	JOIN pg_catalog.pg_namespace AS namespaces ON namespaces.oid = routines.pronamespace
	WHERE namespaces.nspname = 'public'
		AND routines.proname LIKE 'runnerfoundry\_%' ESCAPE '\'
	ORDER BY 1, 2`

func e2ePostgresReadOwnershipInventory(
	ctx context.Context,
	connection *pgx.Conn,
	stableRole string,
) (e2ePostgresOwnershipInventory, error) {
	rows, err := connection.Query(ctx, e2ePostgresOwnershipSQL)
	if err != nil {
		return e2ePostgresOwnershipInventory{}, fmt.Errorf("inspect RunnerFoundry PostgreSQL object ownership: %w", err)
	}
	defer rows.Close()

	inventory := e2ePostgresOwnershipInventory{objectCounts: make(map[string]int)}
	for rows.Next() {
		var objectType string
		var objectName string
		var owner string
		if scanErr := rows.Scan(&objectType, &objectName, &owner); scanErr != nil {
			return e2ePostgresOwnershipInventory{}, fmt.Errorf(
				"scan RunnerFoundry PostgreSQL object ownership: %w", scanErr,
			)
		}
		inventory.objectCounts[objectType]++
		inventory.versionTableFound = inventory.versionTableFound || objectName == "public.runnerfoundry_schema_version"
		if owner != stableRole {
			inventory.ownershipErrors = append(inventory.ownershipErrors,
				fmt.Errorf("%s %s is owned by %s, want %s", objectType, objectName, owner, stableRole))
		}
	}
	if err = rows.Err(); err != nil {
		return e2ePostgresOwnershipInventory{}, fmt.Errorf("iterate RunnerFoundry PostgreSQL object ownership: %w", err)
	}
	return inventory, nil
}

func (inventory e2ePostgresOwnershipInventory) validate() error {
	result := errors.Join(inventory.ownershipErrors...)
	result = errors.Join(result, requirePostgresOwnedObjects(inventory.objectCounts, "table"))
	result = errors.Join(result, requirePostgresOwnedObjects(inventory.objectCounts, "index"))
	result = errors.Join(result, requirePostgresOwnedObjects(inventory.objectCounts, "function"))
	if !inventory.versionTableFound {
		result = errors.Join(result, errors.New("tern metadata table was absent from ownership inventory"))
	}
	return result
}

func requirePostgresOwnedObjects(objectCounts map[string]int, objectType string) error {
	if objectCounts[objectType] == 0 {
		return fmt.Errorf("RunnerFoundry ownership inventory contains no %s objects", objectType)
	}
	return nil
}

func e2ePostgresValidateStableRole(ctx context.Context, connection *pgx.Conn, stableRole string) error {
	var canLogin, superuser, createDatabase, createRole, replication bool
	if err := connection.QueryRow(ctx, `SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication
		FROM pg_catalog.pg_roles WHERE rolname = $1`, stableRole).
		Scan(&canLogin, &superuser, &createDatabase, &createRole, &replication); err != nil {
		return fmt.Errorf("inspect PostgreSQL stable role attributes: %w", err)
	}
	if canLogin || superuser || createDatabase || createRole || replication {
		return fmt.Errorf(
			"PostgreSQL stable role privileges = LOGIN:%t SUPERUSER:%t CREATEDB:%t CREATEROLE:%t REPLICATION:%t",
			canLogin, superuser, createDatabase, createRole, replication,
		)
	}
	return nil
}

func (fixture *e2ePostgresFixture) e2ePostgresAdminConnectionConfig() (*pgx.ConnConfig, error) {
	configuration, err := pgx.ParseConfig(fmt.Sprintf(
		"host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable",
		fixture.port, e2ePostgresAdminDatabase, e2ePostgresAdminUser, e2ePostgresAdminPassword,
	))
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL administrator connection configuration: %w", err)
	}
	return configuration, nil
}

func e2ePostgresVersion(configured string) (embeddedpostgres.PostgresVersion, error) {
	switch configured {
	case "17", "17.5", "17.5.0":
		return embeddedpostgres.V17, nil
	case "", "18", "18.3", "18.3.0":
		return embeddedpostgres.V18, nil
	default:
		return "", fmt.Errorf("unsupported E2E_POSTGRES_VERSION %q: expected 17.5.0 or 18.3.0", configured)
	}
}

func e2ePostgresValidateBinariesPath(path string) error {
	controlProgram := filepath.Join(path, "bin", "pg_ctl")
	if _, err := os.Stat(controlProgram); err != nil {
		return fmt.Errorf("E2E_POSTGRES_BINARIES must contain pre-extracted bin/pg_ctl at %s: %w", controlProgram, err)
	}

	return nil
}

func e2ePostgresFreeLoopbackPort() (uint32, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("listen on an ephemeral loopback port: %w", err)
	}
	address, addressOK := listener.Addr().(*net.TCPAddr)
	if !addressOK {
		addressErr := fmt.Errorf("unexpected loopback listener address %T", listener.Addr())
		if closeErr := listener.Close(); closeErr != nil {
			return 0, errors.Join(addressErr, fmt.Errorf("close loopback listener: %w", closeErr))
		}
		return 0, addressErr
	}
	var port uint32
	if _, err = fmt.Sscan(strconv.Itoa(address.Port), &port); err != nil {
		return 0, fmt.Errorf("parse reserved loopback port: %w", err)
	}
	if err = listener.Close(); err != nil {
		return 0, fmt.Errorf("close loopback listener: %w", err)
	}

	return port, nil
}

func e2ePostgresDatabaseName(testName string) string {
	digest := sha256.Sum256([]byte(testName))

	return fmt.Sprintf("runnerfoundry_e2e_%x", digest[:8])
}
