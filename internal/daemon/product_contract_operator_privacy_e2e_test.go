//go:build e2e

package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/config"
)

func TestProductContractOperatorLogsDescribeLifecycleWithoutPrivateBoundaryValues(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	for _, level := range []string{"info", "debug"} {
		t.Run(level, func(t *testing.T) {
			textOperations := exerciseProductionCompositionLoggingFormat(t, "text", level)
			jsonOperations := exerciseProductionCompositionLoggingFormat(t, "json", level)
			if !reflect.DeepEqual(textOperations, jsonOperations) {
				t.Fatalf("%s lifecycle operations differ between text %v and JSON %v",
					level, textOperations, jsonOperations)
			}
		})
	}
}

func exerciseProductionCompositionLoggingFormat(t *testing.T, format string, level string) []string {
	t.Helper()
	var acceptedLogs string
	t.Run(format+"_accepted", func(t *testing.T) {
		acceptedLogs = exerciseProductionCompositionLogging(t, format, level)
	})
	var retryLogs string
	t.Run(format+"_retry", func(t *testing.T) {
		retryLogs = exerciseProductionCompositionRetryLogging(t, productionCompositionLogConfig(t, format, level))
	})
	raw := acceptedLogs + retryLogs
	assertOperatorLogContract(t, raw, format, level)
	return observedLifecycleOperations(raw)
}

func productionCompositionLogConfig(t *testing.T, format string, level string) config.Config {
	t.Helper()
	configured := loadProductionCompositionConfig(t)
	configured.Log.Format = format
	configured.Log.Level = level
	return configured
}

func exerciseProductionCompositionLogging(t *testing.T, format string, level string) string {
	t.Helper()
	configured := productionCompositionLogConfig(t, format, level)
	host := newProductionCompositionGitHub()
	t.Cleanup(func() { host.assertExpected(t) })
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	runtime, generation, tickers := openProductionComposition(
		t, configured, host, dockerCommands, libvirtCommands,
	)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("observed lifecycle operations before failure: %v", observedLifecycleOperations(tickers.logs.String()))
		}
	})
	exerciseProductionCompositionLifecycle(t, runtime, tickers, host, dockerCommands, libvirtCommands)
	closeProductionComposition(context.Background(), t, runtime, generation)
	return tickers.logs.String()
}

func exerciseProductionCompositionRetryLogging(t *testing.T, configured config.Config) string {
	t.Helper()
	host := newProductionCompositionGitHub()
	t.Cleanup(func() { host.assertExpected(t) })
	dockerCommands := newProductionCompositionDockerCommands()
	libvirtCommands := newProductionCompositionLibvirtCommands()
	releaseFailure := dockerCommands.blockNextStartWithPartial(errors.New("private retry sentinel"))
	runtime, generation, tickers := openProductionComposition(
		t, configured, host, dockerCommands, libvirtCommands,
	)
	exerciseProductionCompositionProvisioningRetry(
		t, runtime, tickers, host, dockerCommands, releaseFailure,
	)
	closeProductionComposition(context.Background(), t, runtime, generation)
	return tickers.logs.String()
}

func assertOperatorLogContract(t *testing.T, raw string, format string, level string) {
	t.Helper()
	assertOperatorLogValues(t, raw, format, level, "lifecycle operation", []string{
		"source_reconciliation", "source_sessions", "capacity_publication", "job_ingestion",
		"claim", "runner_start", "runner_retry",
	})
	assertOperatorLogValues(t, raw, format, level, "operator field", []string{"runner_pool", "outcome", "capacity"})
	if level == "debug" {
		assertOperatorLogValues(t, raw, format, level, "diagnostic field or operation", []string{
			"message_id", "job_id", "assigned_demand", "acknowledge_message", "provider_start", "provider_destroy",
		})
	}
	assertPrivateValuesAbsentFromOperatorLogs(t, raw, format, level)
}

func assertOperatorLogValues(
	t *testing.T,
	raw string,
	format string,
	level string,
	description string,
	values []string,
) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(raw, value) {
			t.Fatalf("%s %s logs omit %s %q", format, level, description, value)
		}
	}
}

func assertPrivateValuesAbsentFromOperatorLogs(t *testing.T, raw string, format string, level string) {
	t.Helper()
	privateValues := []string{
		"production-composition-bootstrap",
		"queue-token",
		"installation-token",
		"registration-token",
		`<domain><name>template</name></domain>`,
		e2ePostgresLoginPassword,
		string(productionCompositionPrivateKey(t)),
	}
	for index, privateValue := range privateValues {
		if privateValue != "" && strings.Contains(raw, privateValue) {
			t.Fatalf("%s %s logs exposed private boundary value %d (length %d)",
				format, level, index, len(privateValue))
		}
	}
}

func observedLifecycleOperations(raw string) []string {
	operations := make([]string, 0)
	for _, operation := range []string{
		"source_reconciliation", "source_sessions", "capacity_publication", "assigned_demand",
		"acquire_jobs", "job_ingestion", "acknowledge_message", "claim", "runner_start", "runner_retry",
		"provider_start", "provider_destroy", "generation",
	} {
		if strings.Contains(raw, operation) {
			operations = append(operations, operation)
		}
	}
	return operations
}
