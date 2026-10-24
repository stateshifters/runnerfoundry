package cluster

import (
	"errors"
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
)

type Settings = postgresconfig.Settings
type Endpoint = postgresconfig.Endpoint

type connectionFixtureValues struct {
	identity string
	proof    string
}

func newConnectionFixtureValues() connectionFixtureValues {
	return connectionFixtureValues{identity: "scaler-fixture-user", proof: "opaque-fixture-value"}
}

func (values connectionFixtureValues) dsn(host string, port string, database string) string {
	return (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(values.identity, values.proof),
		Host:   host + ":" + port,
		Path:   database,
	}).String()
}

func (values connectionFixtureValues) malformedDSN() string {
	return "postgres://" + values.identity + ":" + values.proof + "@host:%zz/name"
}

func settingsWithProofReference(reference string) Settings {
	return Settings{Password: reference, Role: "runnerfoundry"}
}

type structuredConnectionField struct {
	name string
	set  func(*Settings, string)
	get  func(*pgx.ConnConfig) string
	want string
}

func TestSettingsResolveStructuredLiteralFields(t *testing.T) {
	for _, field := range structuredConnectionFields() {
		t.Run(field.name, func(t *testing.T) {
			assertStructuredConnectionField(t, field, field.want)
		})
	}
}

func TestSettingsResolveStructuredEnvironmentReferences(t *testing.T) {
	for _, field := range structuredConnectionFields() {
		t.Run(field.name, func(t *testing.T) {
			environmentName := "SCALER_TEST_" + strings.ToUpper(field.name)
			assertStructuredConnectionField(t, field, "env://"+environmentName)
		})
	}
}

func structuredConnectionFields() []structuredConnectionField {
	values := newConnectionFixtureValues()
	return []structuredConnectionField{
		{name: "host", set: func(settings *Settings, value string) { settings.Host = value }, get: func(config *pgx.ConnConfig) string { return config.Host }, want: "database.internal"},
		{name: "port", set: func(settings *Settings, value string) { settings.Port = value }, get: func(config *pgx.ConnConfig) string { return strconv.Itoa(int(config.Port)) }, want: "6432"},
		{name: "name", set: func(settings *Settings, value string) { settings.Name = value }, get: func(config *pgx.ConnConfig) string { return config.Database }, want: "runnerfoundry"},
		{name: "user", set: func(settings *Settings, value string) { settings.User = value }, get: func(config *pgx.ConnConfig) string { return config.User }, want: values.identity},
		{name: "password", set: func(settings *Settings, value string) { settings.Password = value }, get: func(config *pgx.ConnConfig) string { return config.Password }, want: values.proof},
	}
}

func assertStructuredConnectionField(t *testing.T, field structuredConnectionField, configuredValue string) {
	t.Helper()
	settings := Settings{Role: "runnerfoundry"}
	field.set(&settings, configuredValue)
	environment := map[string]string{}
	if strings.HasPrefix(configuredValue, "env://") {
		environment[strings.TrimPrefix(configuredValue, "env://")] = field.want
	}
	resolved, err := primaryConnectionConfigWithResolver(settings, testConnectionResolver(environment))
	if err != nil {
		t.Fatal(err)
	}
	if got := field.get(resolved); got != field.want {
		t.Fatalf("resolved %s = %q, want %q", field.name, got, field.want)
	}
}

func TestSettingsResolveEveryDSNSource(t *testing.T) {
	values := newConnectionFixtureValues()
	dsn := values.dsn("database.internal", "6432", "runnerfoundry")
	tests := map[string]string{
		"literal":     dsn,
		"environment": "env://SCALER_TEST_DSN",
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			resolved, err := primaryConnectionConfigWithResolver(
				Settings{DSN: source, Role: "runnerfoundry"},
				testConnectionResolver(map[string]string{"SCALER_TEST_DSN": dsn}),
			)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Host != "database.internal" || resolved.Port != 6432 || resolved.Database != "runnerfoundry" || resolved.User != values.identity || resolved.Password != values.proof {
				t.Fatalf("resolved DSN configuration = %#v", resolved)
			}
		})
	}
}

func TestSettingsResolveRoleForEveryConnection(t *testing.T) {
	for sourceName, roleSource := range map[string]string{
		"literal": "runnerfoundry", "environment": "env://SCALER_TEST_ROLE",
	} {
		t.Run(sourceName, func(t *testing.T) {
			resolved, err := primaryConnectionConfigWithResolver(Settings{
				DSN: "postgres://login@primary/scaler?role=wrong", Role: roleSource,
			}, testConnectionResolver(map[string]string{"SCALER_TEST_ROLE": "runnerfoundry"}))
			if err != nil {
				t.Fatal(err)
			}
			if _, present := resolved.RuntimeParams["role"]; present {
				t.Fatalf("unexpected startup role = %q", resolved.RuntimeParams["role"])
			}
		})
	}
}

func TestSettingsBuildDirectConnection(t *testing.T) {
	settings := Settings{
		Host: "pool", Port: "6432", Name: "scaler", User: "login", Password: "secret", Role: "runnerfoundry",
		Direct: &Endpoint{Host: "database", Port: "5432"},
	}
	resolved, err := directConnectionConfigWithResolver(settings, testConnectionResolver(nil))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "database" || resolved.Port != 5432 || resolved.Database != "scaler" || resolved.User != "login" || resolved.Password != "secret" {
		t.Fatalf("direct structured settings = %#v", resolved)
	}
	if _, present := resolved.RuntimeParams["role"]; present {
		t.Fatalf("unexpected direct startup role = %q", resolved.RuntimeParams["role"])
	}
}

func TestSettingsBuildEveryDirectConnectionShape(t *testing.T) {
	const primaryDSN = "postgres://primary-user@primary:5432/primary"
	const directDSN = "postgres://direct-user@direct:6432/direct"
	tests := map[string]struct {
		settings Settings
		host     string
		user     string
	}{
		"no direct endpoint": {
			settings: Settings{DSN: primaryDSN, Role: "runnerfoundry"}, host: "primary", user: "primary-user",
		},
		"empty direct endpoint": {
			settings: Settings{DSN: primaryDSN, Role: "runnerfoundry", Direct: &Endpoint{}}, host: "primary", user: "primary-user",
		},
		"complete direct DSN": {
			settings: Settings{DSN: primaryDSN, Role: "runnerfoundry", Direct: &Endpoint{DSN: directDSN}}, host: "direct", user: "direct-user",
		},
		"direct DSN over structured primary": {
			settings: Settings{Host: "primary", Role: "runnerfoundry", Direct: &Endpoint{DSN: directDSN}}, host: "direct", user: "direct-user",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			resolved, err := directConnectionConfigWithResolver(test.settings, testConnectionResolver(nil))
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Host != test.host || resolved.User != test.user {
				t.Fatalf("direct config host=%q user=%q", resolved.Host, resolved.User)
			}
		})
	}
}

func TestPrimaryConnectionBuilderRejectsStructurallyInvalidSettings(t *testing.T) {
	assertConnectionBuilderError(t, primaryConnectionConfigWithResolver, Settings{Host: "database"},
		"validate PostgreSQL connection settings: state.postgres.role is required")
	assertConnectionBuilderError(t, primaryConnectionConfigWithResolver, Settings{
		Role: "runnerfoundry", DSN: "postgres://pool/scaler", Host: "database",
	}, "validate PostgreSQL connection settings: "+
		"state.postgres.dsn cannot be combined with structured PostgreSQL settings")
}

func TestDirectConnectionBuilderRejectsStructurallyInvalidSettings(t *testing.T) {
	assertConnectionBuilderError(t, directConnectionConfigWithResolver, Settings{
		Role: "runnerfoundry", Host: "primary", Direct: &Endpoint{
			DSN: "postgres://direct-user@direct:6432/direct", Host: "direct",
		},
	}, "validate PostgreSQL connection settings: "+
		"state.postgres.direct.dsn cannot be combined with structured PostgreSQL settings")
	assertConnectionBuilderError(t, directConnectionConfigWithResolver, Settings{
		Role: "runnerfoundry", DSN: "postgres://pool/scaler", Direct: &Endpoint{Host: "database"},
	}, "validate PostgreSQL connection settings: "+
		"state.postgres.direct.dsn is required when overriding a primary DSN")
}

func assertConnectionBuilderError(
	t *testing.T,
	build func(Settings, connectionSettingResolver) (*pgx.ConnConfig, error),
	settings Settings,
	expectedError string,
) {
	t.Helper()
	_, err := build(settings, testConnectionResolver(nil))
	if err == nil || err.Error() != expectedError {
		t.Fatalf("connection builder error = %v, want %q", err, expectedError)
	}
}

func TestSettingsUseStandardPostgreSQLEnvironmentDefaults(t *testing.T) {
	resolver := testConnectionResolver(nil)
	resolver.parseConfig = func(connectionString string) (*pgx.ConnConfig, error) {
		if connectionString != "" {
			return nil, errors.New("unexpected connection string")
		}
		return &pgx.ConnConfig{Config: pgconn.Config{
			Host: "environment-host", Port: 6543, Database: "environment-name",
			User: "environment-user", Password: "environment-password", RuntimeParams: map[string]string{},
		}}, nil
	}
	resolved, err := primaryConnectionConfigWithResolver(Settings{Role: "runnerfoundry"}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "environment-host" || resolved.Port != 6543 || resolved.Database != "environment-name" || resolved.User != "environment-user" || resolved.Password != "environment-password" {
		t.Fatalf("standard PostgreSQL environment configuration = %#v", resolved)
	}
}

func TestSettingsRejectInvalidEnvironmentReferencesWithoutDisclosure(t *testing.T) {
	tests := map[string]Settings{
		"missing role environment": {Host: "database", Role: "env://SCALER_MISSING_ROLE"},
		"missing environment":      settingsWithProofReference("env://SCALER_MISSING_SECRET"),
		"empty environment":        settingsWithProofReference("env://SCALER_EMPTY_SECRET"),
		"malformed env":            settingsWithProofReference("env:SCALER_SECRET"),
		"empty env name":           {Password: "env://", Role: "runnerfoundry"},
		"invalid env name":         settingsWithProofReference("env://BAD/NAME"),
		"referenced DSN":           {DSN: "env://SCALER_MISSING_DSN", Role: "runnerfoundry"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func TestSettingsRejectInvalidPortsWithoutDisclosure(t *testing.T) {
	tests := map[string]Settings{
		"zero port":       {Port: "0", Role: "runnerfoundry"},
		"large port":      {Port: "65536", Role: "runnerfoundry"},
		"nonnumeric port": {Port: "secret-port", Role: "runnerfoundry"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func TestSettingsRejectMalformedDSNWithoutDisclosure(t *testing.T) {
	tests := map[string]Settings{
		"malformed DSN": {DSN: newConnectionFixtureValues().malformedDSN(), Role: "runnerfoundry"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func assertConnectionSettingsRejectedWithoutDisclosure(t *testing.T, settings Settings) {
	t.Helper()
	_, err := primaryConnectionConfigWithResolver(settings, testConnectionResolver(map[string]string{"SCALER_EMPTY_SECRET": ""}))
	if err == nil {
		t.Fatal("expected connection settings to fail")
	}
	for _, forbidden := range []string{"SCALER_", "secret-location", "scaler-secret", newConnectionFixtureValues().proof, "%zz"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error exposed PostgreSQL configuration: %q", err)
		}
	}
}

func testConnectionResolver(environment map[string]string) connectionSettingResolver {
	return connectionSettingResolver{
		readCredentialFile: func(string) ([]byte, error) { return nil, errors.New("missing credential file") },
		localizeCredentialPath: func(filePath string) (string, error) {
			if !path.IsAbs(filePath) {
				return "", errInvalidCredentialFilePath
			}
			return filePath, nil
		},
		lookupEnvironment: func(name string) (string, bool) {
			value, exists := environment[name]
			return value, exists
		},
		parseConfig: parseDeterministicConnectionConfig,
	}
}

func parseDeterministicConnectionConfig(connectionString string) (*pgx.ConnConfig, error) {
	if connectionString == "" {
		return emptyTestConnectionConfig(), nil
	}
	parsed, err := url.Parse(connectionString)
	if err != nil {
		return nil, errors.New("invalid connection string")
	}
	return deterministicConnectionConfigFromURL(parsed)
}

func deterministicConnectionConfigFromURL(parsed *url.URL) (*pgx.ConnConfig, error) {
	config := emptyTestConnectionConfig()
	config.Host = parsed.Hostname()
	config.Database = strings.TrimPrefix(parsed.Path, "/")
	if parsed.User != nil {
		config.User = parsed.User.Username()
		config.Password, _ = parsed.User.Password()
	}
	port, err := deterministicConnectionPort(parsed.Port())
	if err != nil {
		return nil, err
	}
	config.Port = port
	applyDeterministicConnectionQuery(config, parsed.Query())
	queryPort, err := deterministicConnectionPort(parsed.Query().Get("port"))
	if err != nil {
		return nil, err
	}
	if queryPort != 0 {
		config.Port = queryPort
	}
	return config, nil
}

func emptyTestConnectionConfig() *pgx.ConnConfig {
	return &pgx.ConnConfig{Config: pgconn.Config{RuntimeParams: map[string]string{}}}
}

func applyDeterministicConnectionQuery(config *pgx.ConnConfig, values url.Values) {
	for key, destination := range map[string]*string{
		"host": &config.Host, "dbname": &config.Database, "user": &config.User, "password": &config.Password,
	} {
		if value := values.Get(key); value != "" {
			*destination = value
		}
	}
}

func deterministicConnectionPort(value string) (uint16, error) {
	if value == "" {
		return 0, nil
	}
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0, errors.New("invalid connection port")
	}
	return uint16(port), nil
}
