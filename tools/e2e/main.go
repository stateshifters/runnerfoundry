package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type options struct {
	repositoryRoot  string
	postgresRoot    string
	postgresVersion string
	backend         string
	packages        string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(errorExitCode(err))
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("expected prepare or test command")
	}

	switch arguments[0] {
	case "prepare":
		cfg, err := parseOptions("prepare", arguments[1:])
		if err != nil {
			return err
		}
		return prepare(cfg)
	case "test":
		cfg, err := parseOptions("test", arguments[1:])
		if err != nil {
			return err
		}
		return test(cfg)
	default:
		return fmt.Errorf("unsupported command %q: expected prepare or test", arguments[0])
	}
}

func parseOptions(command string, arguments []string) (options, error) {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	cfg := options{}
	flags.StringVar(&cfg.repositoryRoot, "repository-root", "", "RunnerFoundry repository root")
	flags.StringVar(&cfg.postgresRoot, "postgres-root", os.Getenv("E2E_POSTGRES_ROOT"), "prepared PostgreSQL root")
	flags.StringVar(&cfg.postgresVersion, "postgres-version", os.Getenv("E2E_POSTGRES_VERSION"), "PostgreSQL versions")
	if command == "test" {
		flags.StringVar(&cfg.backend, "backend", environmentValue("E2E_BACKEND", "all"), "E2E backend")
		flags.StringVar(&cfg.packages, "packages", environmentValue("E2E_PACKAGES", "./internal/daemon"), "Go packages")
	}
	if err := flags.Parse(arguments); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if cfg.repositoryRoot == "" {
		return options{}, errors.New("--repository-root is required")
	}
	if cfg.postgresRoot == "" {
		cfg.postgresRoot = filepath.Join(cfg.repositoryRoot, "build", "e2e", "postgres")
	} else if !filepath.IsAbs(cfg.postgresRoot) {
		cfg.postgresRoot = filepath.Join(cfg.repositoryRoot, cfg.postgresRoot)
	}
	return cfg, nil
}

func environmentValue(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func runCommand(directory string, name string, arguments ...string) error {
	command := exec.Command(name, arguments...)
	command.Dir = directory
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Stdin = os.Stdin
	return command.Run()
}

func errorExitCode(err error) int {
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return 1
}
