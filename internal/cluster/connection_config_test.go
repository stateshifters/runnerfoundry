package cluster

import (
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/meza/github-scaler/internal/postgresconfig"
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
	return Settings{Password: reference, Role: "github_scaler"}
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
			t.Setenv(environmentName, field.want)
			assertStructuredConnectionField(t, field, "env://"+environmentName)
		})
	}
}

func structuredConnectionFields() []structuredConnectionField {
	values := newConnectionFixtureValues()
	return []structuredConnectionField{
		{name: "host", set: func(settings *Settings, value string) { settings.Host = value }, get: func(config *pgx.ConnConfig) string { return config.Host }, want: "database.internal"},
		{name: "port", set: func(settings *Settings, value string) { settings.Port = value }, get: func(config *pgx.ConnConfig) string { return strconv.Itoa(int(config.Port)) }, want: "6432"},
		{name: "name", set: func(settings *Settings, value string) { settings.Name = value }, get: func(config *pgx.ConnConfig) string { return config.Database }, want: "github_scaler"},
		{name: "user", set: func(settings *Settings, value string) { settings.User = value }, get: func(config *pgx.ConnConfig) string { return config.User }, want: values.identity},
		{name: "password", set: func(settings *Settings, value string) { settings.Password = value }, get: func(config *pgx.ConnConfig) string { return config.Password }, want: values.proof},
	}
}

func assertStructuredConnectionField(t *testing.T, field structuredConnectionField, configuredValue string) {
	t.Helper()
	settings := Settings{Role: "github_scaler"}
	field.set(&settings, configuredValue)
	resolved, err := primaryConnectionConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	if got := field.get(resolved); got != field.want {
		t.Fatalf("resolved %s = %q, want %q", field.name, got, field.want)
	}
}

func TestSettingsResolveEveryDSNSource(t *testing.T) {
	values := newConnectionFixtureValues()
	dsn := values.dsn("database.internal", "6432", "github_scaler")
	t.Setenv("SCALER_TEST_DSN", dsn)
	tests := map[string]string{
		"literal":     dsn,
		"environment": "env://SCALER_TEST_DSN",
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			resolved, err := primaryConnectionConfig(Settings{DSN: source, Role: "github_scaler"})
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Host != "database.internal" || resolved.Port != 6432 || resolved.Database != "github_scaler" || resolved.User != values.identity || resolved.Password != values.proof {
				t.Fatalf("resolved DSN configuration = %#v", resolved)
			}
		})
	}
}

func TestSettingsResolveRoleForEveryConnection(t *testing.T) {
	t.Setenv("SCALER_TEST_ROLE", "github_scaler")
	for sourceName, roleSource := range map[string]string{
		"literal": "github_scaler", "environment": "env://SCALER_TEST_ROLE",
	} {
		t.Run(sourceName, func(t *testing.T) {
			resolved, err := primaryConnectionConfig(Settings{
				DSN: "postgres://login@primary/scaler?role=wrong", Role: roleSource,
			})
			if err != nil {
				t.Fatal(err)
			}
			if resolved.RuntimeParams["role"] != "github_scaler" {
				t.Fatalf("resolved role = %q", resolved.RuntimeParams["role"])
			}
		})
	}
}

func TestSettingsBuildDirectConnection(t *testing.T) {
	settings := Settings{
		Host: "pool", Port: "6432", Name: "scaler", User: "login", Password: "secret", Role: "github_scaler",
		Direct: &Endpoint{Host: "database", Port: "5432"},
	}
	resolved, err := directConnectionConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "database" || resolved.Port != 5432 || resolved.Database != "scaler" || resolved.User != "login" || resolved.Password != "secret" {
		t.Fatalf("direct structured settings = %#v", resolved)
	}
	if resolved.RuntimeParams["role"] != "github_scaler" {
		t.Fatalf("direct role = %q", resolved.RuntimeParams["role"])
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
			settings: Settings{DSN: primaryDSN, Role: "github_scaler"}, host: "primary", user: "primary-user",
		},
		"empty direct endpoint": {
			settings: Settings{DSN: primaryDSN, Role: "github_scaler", Direct: &Endpoint{}}, host: "primary", user: "primary-user",
		},
		"complete direct DSN": {
			settings: Settings{DSN: primaryDSN, Role: "github_scaler", Direct: &Endpoint{DSN: directDSN}}, host: "direct", user: "direct-user",
		},
		"direct DSN over structured primary": {
			settings: Settings{Host: "primary", Role: "github_scaler", Direct: &Endpoint{DSN: directDSN}}, host: "direct", user: "direct-user",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			resolved, err := directConnectionConfig(test.settings)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Host != test.host || resolved.User != test.user {
				t.Fatalf("direct config host=%q user=%q", resolved.Host, resolved.User)
			}
		})
	}
}

func TestSettingsRejectDirectDSNWithStructuredOverrides(t *testing.T) {
	const directDSN = "postgres://direct-user@direct:6432/direct"
	if _, err := directConnectionConfig(Settings{
		Host: "primary", Role: "github_scaler",
		Direct: &Endpoint{DSN: directDSN, Host: "direct"},
	}); err == nil {
		t.Fatal("expected direct DSN and structured settings conflict")
	}
}

func TestSettingsRejectMissingRoleAndOpaqueDSNPartialDirectOverride(t *testing.T) {
	for name, settings := range map[string]Settings{
		"missing role": {Host: "database"},
		"opaque DSN partial direct override": {
			DSN: "postgres://pool/scaler", Role: "github_scaler", Direct: &Endpoint{Host: "database"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := directConnectionConfig(settings); err == nil {
				t.Fatal("expected direct connection settings error")
			}
		})
	}
}

func TestSettingsUseStandardPostgreSQLEnvironmentDefaults(t *testing.T) {
	t.Setenv("PGHOST", "environment-host")
	t.Setenv("PGPORT", "6543")
	t.Setenv("PGDATABASE", "environment-name")
	t.Setenv("PGUSER", "environment-user")
	t.Setenv("PGPASSWORD", "environment-password")
	resolved, err := primaryConnectionConfig(Settings{Role: "github_scaler"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "environment-host" || resolved.Port != 6543 || resolved.Database != "environment-name" || resolved.User != "environment-user" || resolved.Password != "environment-password" {
		t.Fatalf("standard PostgreSQL environment configuration = %#v", resolved)
	}
}

func TestSettingsRejectInvalidEnvironmentReferencesWithoutDisclosure(t *testing.T) {
	t.Setenv("SCALER_EMPTY_SECRET", "")
	tests := map[string]Settings{
		"missing role environment": {Host: "database", Role: "env://SCALER_MISSING_ROLE"},
		"missing environment":      settingsWithProofReference("env://SCALER_MISSING_SECRET"),
		"empty environment":        settingsWithProofReference("env://SCALER_EMPTY_SECRET"),
		"malformed env":            settingsWithProofReference("env:SCALER_SECRET"),
		"empty env name":           {Password: "env://", Role: "github_scaler"},
		"invalid env name":         settingsWithProofReference("env://BAD/NAME"),
		"referenced DSN":           {DSN: "env://SCALER_MISSING_DSN", Role: "github_scaler"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func TestSettingsRejectInvalidPortsWithoutDisclosure(t *testing.T) {
	tests := map[string]Settings{
		"zero port":       {Port: "0", Role: "github_scaler"},
		"large port":      {Port: "65536", Role: "github_scaler"},
		"nonnumeric port": {Port: "secret-port", Role: "github_scaler"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func TestSettingsRejectMalformedDSNWithoutDisclosure(t *testing.T) {
	tests := map[string]Settings{
		"malformed DSN": {DSN: newConnectionFixtureValues().malformedDSN(), Role: "github_scaler"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func assertConnectionSettingsRejectedWithoutDisclosure(t *testing.T, settings Settings) {
	t.Helper()
	_, err := primaryConnectionConfig(settings)
	if err == nil {
		t.Fatal("expected connection settings to fail")
	}
	for _, forbidden := range []string{"SCALER_", "secret-location", "scaler-secret", newConnectionFixtureValues().proof, "%zz"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error exposed PostgreSQL configuration: %q", err)
		}
	}
}
