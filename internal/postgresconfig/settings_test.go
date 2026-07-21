package postgresconfig

import (
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestValidateAcceptsUnambiguousSettings(t *testing.T) {
	valid := []Settings{
		{Role: "github_scaler"},
		{Role: "github_scaler", DSN: "postgres://database/scaler"},
		{Role: "github_scaler", Host: "database", Direct: &Endpoint{Host: "direct"}},
	}
	for _, settings := range valid {
		if err := Validate(settings); err != nil {
			t.Fatalf("Validate(%+v) error = %v", settings, err)
		}
	}
}

func TestValidateRejectsConflictingSettings(t *testing.T) {
	invalid := []Settings{
		{},
		{Role: "github_scaler", DSN: "postgres://database/scaler", Host: "database"},
		{Role: "github_scaler", Direct: &Endpoint{DSN: "postgres://direct/scaler", Host: "direct"}},
		{Role: "github_scaler", DSN: "postgres://database/scaler", Direct: &Endpoint{Host: "direct"}},
	}
	for _, settings := range invalid {
		if err := Validate(settings); err == nil {
			t.Fatalf("Validate(%+v) succeeded, want conflict", settings)
		}
	}
}

func TestConnectionSettingsResolveEveryStructuredFieldSource(t *testing.T) {
	type fieldCase struct {
		name string
		set  func(*Settings, string)
		get  func(*pgx.ConnConfig) string
		want string
	}
	fields := []fieldCase{
		{name: "host", set: func(settings *Settings, value string) { settings.Host = value }, get: func(config *pgx.ConnConfig) string { return config.Host }, want: "database.internal"},
		{name: "port", set: func(settings *Settings, value string) { settings.Port = value }, get: func(config *pgx.ConnConfig) string { return strconv.Itoa(int(config.Port)) }, want: "6432"},
		{name: "name", set: func(settings *Settings, value string) { settings.Name = value }, get: func(config *pgx.ConnConfig) string { return config.Database }, want: "github_scaler"},
		{name: "user", set: func(settings *Settings, value string) { settings.User = value }, get: func(config *pgx.ConnConfig) string { return config.User }, want: "scaler"},
		{name: "password", set: func(settings *Settings, value string) { settings.Password = value }, get: func(config *pgx.ConnConfig) string { return config.Password }, want: "opaque secret"},
	}
	for _, field := range fields {
		field := field
		t.Run(field.name, func(t *testing.T) {
			for _, source := range []string{"literal", "environment", "file"} {
				source := source
				t.Run(source, func(t *testing.T) {
					configuredValue := field.want
					switch source {
					case "environment":
						environmentName := "SCALER_TEST_" + strings.ToUpper(field.name)
						t.Setenv(environmentName, field.want)
						configuredValue = "env://" + environmentName
					case "file":
						secretPath := filepath.Join(t.TempDir(), field.name)
						if err := os.WriteFile(secretPath, []byte(field.want+"\n"), 0o600); err != nil {
							t.Fatal(err)
						}
						configuredValue = credentialFileURL(secretPath)
					}
					settings := Settings{Role: "github_scaler"}
					field.set(&settings, configuredValue)
					resolved, err := settings.Primary()
					if err != nil {
						t.Fatal(err)
					}
					if got := field.get(resolved); got != field.want {
						t.Fatalf("resolved %s = %q, want %q", field.name, got, field.want)
					}
				})
			}
		})
	}
}

func TestConnectionSettingsResolveEveryDSNSource(t *testing.T) {
	const dsn = "postgres://scaler:secret@database.internal:6432/github_scaler"
	t.Setenv("SCALER_TEST_DSN", dsn)
	secretPath := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(secretPath, []byte(dsn+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := map[string]string{
		"literal":     dsn,
		"environment": "env://SCALER_TEST_DSN",
		"file":        credentialFileURL(secretPath),
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			resolved, err := (Settings{DSN: source, Role: "github_scaler"}).Primary()
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Host != "database.internal" || resolved.Port != 6432 || resolved.Database != "github_scaler" || resolved.User != "scaler" || resolved.Password != "secret" {
				t.Fatalf("resolved DSN configuration = %#v", resolved)
			}
		})
	}
}

func TestConnectionSettingsResolveRoleForEveryConnection(t *testing.T) {
	t.Setenv("SCALER_TEST_ROLE", "github_scaler")
	rolePath := filepath.Join(t.TempDir(), "role")
	if err := os.WriteFile(rolePath, []byte("github_scaler\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for sourceName, roleSource := range map[string]string{
		"literal": "github_scaler", "environment": "env://SCALER_TEST_ROLE", "file": credentialFileURL(rolePath),
	} {
		t.Run(sourceName, func(t *testing.T) {
			resolved, err := (Settings{
				DSN: "postgres://login@primary/scaler?role=wrong", Role: roleSource,
			}).Primary()
			if err != nil {
				t.Fatal(err)
			}
			if resolved.RuntimeParams["role"] != "github_scaler" {
				t.Fatalf("resolved role = %q", resolved.RuntimeParams["role"])
			}
		})
	}
}

func TestConnectionSettingsBuildDirectConnection(t *testing.T) {
	settings := Settings{
		Host: "pool", Port: "6432", Name: "scaler", User: "login", Password: "secret", Role: "github_scaler",
		Direct: &Endpoint{Host: "database", Port: "5432"},
	}
	resolved, err := settings.DirectConfig()
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

func TestConnectionSettingsBuildEveryDirectConnectionShape(t *testing.T) {
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
			resolved, err := test.settings.DirectConfig()
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Host != test.host || resolved.User != test.user {
				t.Fatalf("direct config host=%q user=%q", resolved.Host, resolved.User)
			}
		})
	}
}

func TestConnectionSettingsRejectDirectDSNWithStructuredOverrides(t *testing.T) {
	const directDSN = "postgres://direct-user@direct:6432/direct"
	if _, err := (Settings{
		Host: "primary", Role: "github_scaler",
		Direct: &Endpoint{DSN: directDSN, Host: "direct"},
	}).DirectConfig(); err == nil {
		t.Fatal("expected direct DSN and structured settings conflict")
	}
}

func TestConnectionSettingsRejectMissingRoleAndOpaqueDSNPartialDirectOverride(t *testing.T) {
	for name, settings := range map[string]Settings{
		"missing role": {Host: "database"},
		"opaque DSN partial direct override": {
			DSN: "postgres://pool/scaler", Role: "github_scaler", Direct: &Endpoint{Host: "database"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := settings.DirectConfig(); err == nil {
				t.Fatal("expected direct connection settings error")
			}
		})
	}
}

func TestConnectionSettingsReadsFilesAtPointOfUse(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(secretPath, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	settings := Settings{Password: credentialFileURL(secretPath), Role: "github_scaler"}
	first, firstErr := settings.Primary()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if err := os.WriteFile(secretPath, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, secondErr := settings.Primary()
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	if first.Password != "first" || second.Password != "second" {
		t.Fatalf("file-backed connection configurations = %#v, %#v", first, second)
	}
}

func TestConnectionSettingsUsesStandardPostgreSQLEnvironmentDefaults(t *testing.T) {
	t.Setenv("PGHOST", "environment-host")
	t.Setenv("PGPORT", "6543")
	t.Setenv("PGDATABASE", "environment-name")
	t.Setenv("PGUSER", "environment-user")
	t.Setenv("PGPASSWORD", "environment-password")
	resolved, err := (Settings{Role: "github_scaler"}).Primary()
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "environment-host" || resolved.Port != 6543 || resolved.Database != "environment-name" || resolved.User != "environment-user" || resolved.Password != "environment-password" {
		t.Fatalf("standard PostgreSQL environment configuration = %#v", resolved)
	}
}

func TestConnectionSettingsRejectInvalidReferencesAndPortsWithoutDisclosure(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "secret-location")
	if err := os.Mkdir(secretPath, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCALER_EMPTY_SECRET", "")
	tests := map[string]Settings{
		"missing role environment": {Host: "database", Role: "env://SCALER_MISSING_ROLE"},
		"malformed role file":      {Host: "database", Role: "file://remote/role"},
		"missing environment":      {Password: "env://SCALER_MISSING_SECRET", Role: "github_scaler"},
		"empty environment":        {Password: "env://SCALER_EMPTY_SECRET", Role: "github_scaler"},
		"missing file":             {Password: "file:///definitely/missing/scaler-secret", Role: "github_scaler"},
		"unreadable file":          {Password: credentialFileURL(secretPath), Role: "github_scaler"},
		"malformed env":            {Password: "env:SCALER_SECRET", Role: "github_scaler"},
		"empty env name":           {Password: "env://", Role: "github_scaler"},
		"invalid env name":         {Password: "env://BAD/NAME", Role: "github_scaler"},
		"malformed file":           {Password: "file://", Role: "github_scaler"},
		"file host":                {Password: "file://remote/secret", Role: "github_scaler"},
		"file fragment":            {Password: "file:///secret#value", Role: "github_scaler"},
		"fallback query":           {Password: "file:///secret?fallback=value", Role: "github_scaler"},
		"empty file":               {Password: writeCredentialReference(t, "empty", " \n"), Role: "github_scaler"},
		"zero port":                {Port: "0", Role: "github_scaler"},
		"large port":               {Port: "65536", Role: "github_scaler"},
		"nonnumeric port":          {Port: "secret-port", Role: "github_scaler"},
		"malformed DSN":            {DSN: "postgres://user:revealed-secret@host:%zz/name", Role: "github_scaler"},
		"referenced DSN":           {DSN: "env://SCALER_MISSING_DSN", Role: "github_scaler"},
		"short file scheme":        {Password: "file:secret", Role: "github_scaler"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := settings.Primary()
			if err == nil {
				t.Fatal("expected connection settings to fail")
			}
			for _, forbidden := range []string{"SCALER_", "secret-location", "scaler-secret", "revealed-secret", "%zz"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("error exposed PostgreSQL configuration: %q", err)
				}
			}
		})
	}
}

func writeCredentialReference(t *testing.T, name string, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return credentialFileURL(path)
}

func credentialFileURL(path string) string {
	urlPath := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	return (&url.URL{Scheme: "file", Path: urlPath}).String()
}
