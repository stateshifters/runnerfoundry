package postgresconfig

import "testing"

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
