// Package buildinfo exposes the identity embedded in a RunnerFoundry binary.
package buildinfo

var version = "dev"

// Version returns the semantic version supplied at build time, or "dev" for
// binaries built without release linker flags.
func Version() string {
	return version
}
