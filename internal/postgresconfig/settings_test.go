package postgresconfig

import "testing"

func TestValidateAcceptsUnambiguousSettings(t *testing.T) {
	tests := []struct {
		name     string
		settings Settings
	}{
		{name: "standard PostgreSQL environment", settings: Settings{Role: "runnerfoundry"}},
		{name: "primary DSN", settings: Settings{Role: "runnerfoundry", DSN: "postgres://database/scaler"}},
		{name: "structured primary and direct endpoints", settings: Settings{
			Role: "runnerfoundry", Host: "pool", Direct: &Endpoint{Host: "database"},
		}},
		{name: "structured primary with direct DSN", settings: Settings{
			Role: "runnerfoundry", Host: "pool", Direct: &Endpoint{DSN: "postgres://database/scaler"},
		}},
		{name: "primary DSN with empty direct endpoint", settings: Settings{
			Role: "runnerfoundry", DSN: "postgres://pool/scaler", Direct: &Endpoint{},
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := Validate(testCase.settings); err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateRejectsMissingRole(t *testing.T) {
	expectedError := "state.postgres.role is required"
	assertValidationError(t, Settings{}, expectedError)
	assertValidationError(t, Settings{Role: "   "}, expectedError)
}

func TestValidateRejectsPrimaryDSNWithStructuredSettings(t *testing.T) {
	expectedError := "state.postgres.dsn cannot be combined with structured PostgreSQL settings"
	assertValidationError(t, Settings{
		Role: "runnerfoundry", DSN: "postgres://database/scaler", Host: "database",
	}, expectedError)
	assertValidationError(t, Settings{
		Role: "runnerfoundry", DSN: "   ", Host: "database",
	}, expectedError)
}

func TestValidateRejectsDirectDSNWithStructuredSettings(t *testing.T) {
	expectedError := "state.postgres.direct.dsn cannot be combined with structured PostgreSQL settings"
	assertValidationError(t, Settings{
		Role: "runnerfoundry", Direct: &Endpoint{DSN: "postgres://direct/scaler", Host: "direct"},
	}, expectedError)
	assertValidationError(t, Settings{
		Role: "runnerfoundry", Direct: &Endpoint{DSN: "   ", Host: "direct"},
	}, expectedError)
}

func TestValidateRejectsPrimaryDSNWithPartialDirectOverride(t *testing.T) {
	assertValidationError(t, Settings{
		Role: "runnerfoundry", DSN: "postgres://pool/scaler", Direct: &Endpoint{Host: "direct"},
	}, "state.postgres.direct.dsn is required when overriding a primary DSN")
}

func assertValidationError(t *testing.T, settings Settings, expectedError string) {
	t.Helper()
	err := Validate(settings)
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Validate() error = %v, want %q", err, expectedError)
	}
}
