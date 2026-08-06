package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/meza/RunnerFoundry/internal/daemon"
)

type migrationAction string

const (
	migrationStatusAction migrationAction = "status"
	migrationUpAction     migrationAction = "up"
	migrationDownAction   migrationAction = "down"
)

type migrationRequest struct {
	action     migrationAction
	configPath string
	target     int32
}

type daemonRequest struct {
	configPath         string
	activeRunnerPolicy daemon.ActiveRunnerPolicy
}

func parseDaemonRequest(args []string, output io.Writer) (daemonRequest, error) {
	flags := flag.NewFlagSet("runnerfoundry", flag.ContinueOnError)
	flags.SetOutput(output)
	configPath := flags.String("config", "", "path to scaler YAML config")
	activeRunnerPolicy := flags.String(
		"active-runner-policy",
		string(daemon.LeaveActiveRunners),
		"action for active runners during shutdown or incompatible reload: leave or terminate",
	)
	if err := flags.Parse(args); err != nil {
		return daemonRequest{}, fmt.Errorf("parse flags: %w", err)
	}
	if *configPath == "" {
		return daemonRequest{}, errors.New("--config is required")
	}
	policy := daemon.ActiveRunnerPolicy(*activeRunnerPolicy)
	if policy != daemon.LeaveActiveRunners && policy != daemon.TerminateActiveRunners {
		return daemonRequest{}, errors.New("active runner policy must be leave or terminate")
	}
	return daemonRequest{
		configPath:         *configPath,
		activeRunnerPolicy: policy,
	}, nil
}

func parseMigrationRequest(args []string, output io.Writer) (migrationRequest, error) {
	if len(args) == 0 {
		return migrationRequest{}, errors.New("migration command is required")
	}
	action := migrationAction(args[0])
	if action != migrationStatusAction && action != migrationUpAction && action != migrationDownAction {
		return migrationRequest{}, fmt.Errorf("unsupported migration command %q", action)
	}
	configPath, targetText, err := parseMigrationFlags(action, args[1:], output)
	if err != nil {
		return migrationRequest{}, err
	}
	request := migrationRequest{action: action, configPath: configPath}
	if action == migrationDownAction {
		return parseDownMigrationRequest(request, targetText)
	}
	if targetText != "" {
		return migrationRequest{}, errors.New("--to is only valid for migrate down")
	}
	return request, nil
}

func parseMigrationFlags(
	action migrationAction,
	args []string,
	output io.Writer,
) (configPathValue string, targetValue string, resultErr error) {
	flags := flag.NewFlagSet("runnerfoundry migrate "+string(action), flag.ContinueOnError)
	flags.SetOutput(output)
	configPath := flags.String("config", "", "path to scaler YAML config")
	targetText := flags.String("to", "", "migration version to roll back to")
	if err := flags.Parse(args); err != nil {
		return "", "", fmt.Errorf("parse migration flags: %w", err)
	}
	if *configPath == "" {
		return "", "", errors.New("--config is required")
	}
	return *configPath, *targetText, nil
}

func parseDownMigrationRequest(request migrationRequest, targetText string) (migrationRequest, error) {
	if targetText == "" {
		return migrationRequest{}, errors.New("--to is required for migrate down")
	}
	target, err := strconv.ParseInt(targetText, 10, 32)
	if err != nil {
		return migrationRequest{}, errors.New("--to must be a migration version")
	}
	request.target = int32(target)
	return request, nil
}
