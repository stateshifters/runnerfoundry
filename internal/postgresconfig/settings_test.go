package postgresconfig

import "testing"

func TestValidateAcceptsUnambiguousSettings(t *testing.T) {
	valid := []Settings{
		{Role: "runnerfoundry"},
		{Role: "runnerfoundry", DSN: "postgres://database/scaler"},
		{Role: "runnerfoundry", Host: "database", Direct: &Endpoint{Host: "direct"}},
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
		{Role: "runnerfoundry", DSN: "postgres://database/scaler", Host: "database"},
		{Role: "runnerfoundry", Direct: &Endpoint{DSN: "postgres://direct/scaler", Host: "direct"}},
		{Role: "runnerfoundry", DSN: "postgres://database/scaler", Direct: &Endpoint{Host: "direct"}},
	}
	for _, settings := range invalid {
		if err := Validate(settings); err == nil {
			t.Fatalf("Validate(%+v) succeeded, want conflict", settings)
		}
	}
}
