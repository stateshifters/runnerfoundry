package cluster

import (
	"testing"

	"github.com/meza/github-scaler/internal/config"
)

func TestNewStoreFromConfigPreservesPrimaryAndDirectReferences(t *testing.T) {
	state := config.PostgreSQLStateConfig{
		DSN: "env://PRIMARY_DSN", Role: "file:///role",
		Direct: config.PostgreSQLConnectionConfig{
			Host: "direct", Port: "5432", Name: "scaler", User: "file:///user", Password: "file:///password",
		},
	}
	store := NewStoreFromConfig(state)
	if store.settings.DSN != state.DSN || store.settings.Role != state.Role || store.settings.Direct == nil {
		t.Fatalf("primary settings were not preserved: %#v", store.settings)
	}
	if store.settings.Direct.Host != state.Direct.Host || store.settings.Direct.Port != state.Direct.Port ||
		store.settings.Direct.Name != state.Direct.Name || store.settings.Direct.User != state.Direct.User ||
		store.settings.Direct.Password != state.Direct.Password {
		t.Fatalf("direct settings were not preserved: %#v", store.settings.Direct)
	}
}
