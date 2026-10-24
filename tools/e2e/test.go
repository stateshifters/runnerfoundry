package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

func test(cfg options) error {
	packages := strings.Fields(cfg.packages)
	if len(packages) == 0 {
		return errors.New("at least one E2E package is required")
	}

	switch cfg.backend {
	case "sqlite":
		return runE2E(cfg.repositoryRoot, packages, "sqlite", "", "")
	case "postgres":
		return testPostgres(cfg, packages)
	case "all", "":
		if err := runE2E(cfg.repositoryRoot, packages, "sqlite", "", ""); err != nil {
			return err
		}
		return testPostgres(cfg, packages)
	default:
		return fmt.Errorf("unsupported E2E_BACKEND %q: expected all, sqlite, or postgres", cfg.backend)
	}
}

func testPostgres(cfg options, packages []string) error {
	versions, err := postgresVersions(cfg.postgresVersion)
	if err != nil {
		return err
	}
	for _, version := range versions {
		binaries := postgresDestination(cfg.postgresRoot, version, runtime.GOOS, runtime.GOARCH)
		if !postgresPrepared(binaries) {
			return fmt.Errorf(
				"PostgreSQL %s binaries are not prepared at %s; run: make prepare-e2e E2E_POSTGRES_VERSION=%s",
				version, binaries, version,
			)
		}
		if err = runE2E(cfg.repositoryRoot, packages, "postgres", version, binaries); err != nil {
			return err
		}
	}
	return nil
}

func runE2E(repositoryRoot string, packages []string, backend string, version string, binaries string) error {
	if backend == "sqlite" {
		fmt.Println("Running product-contract E2E tests with SQLite")
	} else {
		fmt.Printf("Running product-contract E2E tests with PostgreSQL %s\n", version)
	}

	arguments := append([]string{"test", "-tags=e2e"}, packages...)
	arguments = append(arguments, "-count=1", "-timeout=30m")
	command := exec.Command("go", arguments...)
	command.Dir = repositoryRoot
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Stdin = os.Stdin
	command.Env = append(os.Environ(),
		"E2E_BACKEND="+backend,
		"E2E_POSTGRES_VERSION="+version,
		"E2E_POSTGRES_BINARIES="+binaries,
	)
	return command.Run()
}
