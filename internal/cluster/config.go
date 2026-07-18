package cluster

import "github.com/meza/github-scaler/internal/config"

// NewStoreFromConfig preserves credential references so every primary and direct
// connection resolves current values and assumes the configured stable role.
func NewStoreFromConfig(state config.PostgreSQLStateConfig) *Store {
	direct := state.Direct
	return NewStore(ConnectionSettings{
		DSN: state.DSN, Host: state.Host, Port: state.Port, Name: state.Name,
		User: state.User, Password: state.Password, Role: state.Role,
		Direct: &ConnectionEndpoint{
			DSN: direct.DSN, Host: direct.Host, Port: direct.Port, Name: direct.Name,
			User: direct.User, Password: direct.Password,
		},
	})
}
