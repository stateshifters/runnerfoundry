package daemon

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/libtnb/logrotate"
	"github.com/stateshifters/runnerfoundry/internal/config"
)

func TestProductionLogFactoryDelegatesRotatingWriterConstruction(t *testing.T) {
	called := false
	openRotatingLogFile := func(string, ...logrotate.Option) (*logrotate.Writer, error) {
		called = true
		return &logrotate.Writer{}, nil
	}
	factory := newProductionLogDestinationFactoryWithRotator(openRotatingLogFile)
	if _, err := factory.openRotatingFile(rotatingFileSettings{}, func(error) {}); err != nil || !called {
		t.Fatalf("rotating writer = (called %t, error %v)", called, err)
	}
}

func TestLogDestinationFactoryUsesStandardOutputWithoutFileConfiguration(t *testing.T) {
	standardOutput := &bytes.Buffer{}
	factory := productionLogDestinationFactory{
		standardOutput: standardOutput,
		ensureDirectory: func(string, fs.FileMode) error {
			t.Fatal("stdout logging attempted to create a directory")
			return nil
		},
		openRotatingFile: func(rotatingFileSettings, func(error)) (io.WriteCloser, error) {
			t.Fatal("stdout logging attempted to open a rotating file")
			return nil, errors.New("unexpected rotating file request")
		},
	}
	destination, err := factory.Open(config.LogConfig{})
	if err != nil {
		t.Fatalf("open standard output destination: %v", err)
	}
	if _, err = destination.Write([]byte("stdout log")); err != nil {
		t.Fatalf("write standard output destination: %v", err)
	}
	if standardOutput.String() != "stdout log" {
		t.Fatalf("standard output = %q", standardOutput.String())
	}
}

func TestLogDestinationFactoryWithExistingParentConfiguresBoundedFileRotation(t *testing.T) {
	logPath := testDaemonAbsolutePath("logs", "runnerfoundry.log")
	standardError := &bytes.Buffer{}
	openedDestination := &recordingLogDestination{}
	var captured rotatingFileSettings
	factory := boundedRotationFactory(t, logPath, standardError, openedDestination, &captured)
	maxSize := 25
	maxBackups := 3
	destination, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: logPath, MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err != nil {
		t.Fatalf("open rotating file destination: %v", err)
	}
	assertBoundedRotation(t, destination, openedDestination, captured, logPath, standardError.String())
}

func TestLogDestinationFactoryCreatesParentBeforeOpeningRotator(t *testing.T) {
	logPath := testDaemonAbsolutePath("missing", "nested", "runnerfoundry.log")
	maxSize := 1
	maxBackups := 1
	operations := []string{}
	factory := productionLogDestinationFactory{
		ensureDirectory: func(path string, mode fs.FileMode) error {
			operations = append(operations, fmt.Sprintf("ensure %s %04o", path, mode))
			return nil
		},
		openRotatingFile: func(rotatingFileSettings, func(error)) (io.WriteCloser, error) {
			operations = append(operations, "open rotator")
			return &recordingLogDestination{}, nil
		},
	}
	_, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: logPath, MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err != nil {
		t.Fatalf("open file logging: %v", err)
	}
	expected := []string{fmt.Sprintf("ensure %s 0750", filepath.Dir(logPath)), "open rotator"}
	if !slices.Equal(operations, expected) {
		t.Fatalf("operations = %#v, want %#v", operations, expected)
	}
}

func boundedRotationFactory(
	t *testing.T,
	logPath string,
	standardError io.Writer,
	openedDestination io.WriteCloser,
	captured *rotatingFileSettings,
) productionLogDestinationFactory {
	t.Helper()
	return productionLogDestinationFactory{
		standardError: standardError,
		ensureDirectory: func(path string, mode fs.FileMode) error {
			if path != filepath.Dir(logPath) {
				t.Fatalf("inspected path = %q", path)
			}
			if mode != 0o750 {
				t.Fatalf("directory mode = %04o", mode)
			}
			return nil
		},
		openRotatingFile: func(settings rotatingFileSettings, reportError func(error)) (io.WriteCloser, error) {
			*captured = settings
			reportError(errors.New("cleanup failed"))
			return openedDestination, nil
		},
	}
}

func assertBoundedRotation(
	t *testing.T,
	destination io.WriteCloser,
	openedDestination io.WriteCloser,
	captured rotatingFileSettings,
	logPath string,
	standardError string,
) {
	t.Helper()
	if destination != openedDestination {
		t.Fatal("factory did not return the rotating destination")
	}
	if captured.Path != logPath || captured.MaxSizeBytes != 25*1024*1024 || captured.MaxBackups != 3 || captured.FileMode != 0o600 {
		t.Fatalf("rotating settings = %#v", captured)
	}
	if !strings.Contains(standardError, "log file maintenance failed: cleanup failed") {
		t.Fatalf("standard error = %q", standardError)
	}
}

func TestLogDestinationFactoryRejectsUnavailableParentDirectory(t *testing.T) {
	maxSize := 1
	maxBackups := 1
	rotatorOpened := false
	factory := productionLogDestinationFactory{
		ensureDirectory: func(string, fs.FileMode) error { return errors.New("permission denied") },
		openRotatingFile: func(rotatingFileSettings, func(error)) (io.WriteCloser, error) {
			rotatorOpened = true
			return &recordingLogDestination{}, nil
		},
	}
	_, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: testDaemonAbsolutePath("missing", "runnerfoundry.log"), MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err == nil || !strings.Contains(err.Error(), "create log file directory") || rotatorOpened {
		t.Fatalf("open error = %v", err)
	}
}

func TestLogDestinationFactoryRejectsParentPathThatIsNotADirectory(t *testing.T) {
	maxSize := 1
	maxBackups := 1
	factory := productionLogDestinationFactory{ensureDirectory: func(string, fs.FileMode) error {
		return errors.New("not a directory")
	}}
	_, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: testDaemonAbsolutePath("not-a-directory", "runnerfoundry.log"), MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("open error = %v", err)
	}
}

func TestLogDestinationFactoryDoesNotFailOpeningWhenMaintenanceErrorReportingFails(t *testing.T) {
	maxSize := 1
	maxBackups := 1
	openedDestination := &recordingLogDestination{}
	factory := productionLogDestinationFactory{
		standardError: errorWriter{err: errors.New("stderr unavailable")},
		ensureDirectory: func(string, fs.FileMode) error {
			return nil
		},
		openRotatingFile: func(_ rotatingFileSettings, reportError func(error)) (io.WriteCloser, error) {
			reportError(errors.New("rotation failed"))
			return openedDestination, nil
		},
	}
	destination, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: testDaemonAbsolutePath("logs", "runnerfoundry.log"), MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err != nil || destination != openedDestination {
		t.Fatalf("destination=%T error=%v", destination, err)
	}
}

func TestLogDestinationFactoryReportsRotatorOpenFailure(t *testing.T) {
	maxSize := 1
	maxBackups := 1
	factory := productionLogDestinationFactory{
		ensureDirectory: func(string, fs.FileMode) error { return nil },
		openRotatingFile: func(rotatingFileSettings, func(error)) (io.WriteCloser, error) {
			return nil, errors.New("open failed")
		},
	}
	_, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: testDaemonAbsolutePath("logs", "runnerfoundry.log"), MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err == nil || !strings.Contains(err.Error(), "open failed") {
		t.Fatalf("open error = %v", err)
	}
}

func testDaemonAbsolutePath(elements ...string) string {
	if filepath.Separator == '\\' {
		return filepath.Join(append([]string{"C:" + string(filepath.Separator)}, elements...)...)
	}
	return filepath.Join(append([]string{string(filepath.Separator)}, elements...)...)
}

type recordingLogDestination struct {
	bytes.Buffer
	closeCalls int
}

func (destination *recordingLogDestination) Close() error {
	destination.closeCalls++
	return nil
}

type errorWriter struct{ err error }

func (writer errorWriter) Write([]byte) (int, error) { return 0, writer.err }
