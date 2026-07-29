package daemon

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestLogDestinationFactoryUsesStandardOutputWithoutFileConfiguration(t *testing.T) {
	standardOutput := &bytes.Buffer{}
	factory := productionLogDestinationFactory{standardOutput: standardOutput}
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

func TestLogDestinationFactoryConfiguresBoundedFileRotation(t *testing.T) {
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
		inspectPath: func(path string) (fs.FileInfo, error) {
			if path != filepath.Dir(logPath) {
				t.Fatalf("inspected path = %q", path)
			}
			return fakeFileInfo{directory: true}, nil
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
	factory := productionLogDestinationFactory{inspectPath: func(string) (fs.FileInfo, error) {
		return nil, errors.New("missing directory")
	}}
	_, err := factory.Open(config.LogConfig{File: &config.LogFileConfig{
		Path: testDaemonAbsolutePath("missing", "runnerfoundry.log"), MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err == nil || !strings.Contains(err.Error(), "inspect log file directory") {
		t.Fatalf("open error = %v", err)
	}
}

func TestLogDestinationFactoryReportsRotatorOpenFailure(t *testing.T) {
	maxSize := 1
	maxBackups := 1
	factory := productionLogDestinationFactory{
		inspectPath: func(string) (fs.FileInfo, error) { return fakeFileInfo{directory: true}, nil },
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

type fakeFileInfo struct {
	directory bool
}

func (fakeFileInfo) Name() string       { return "logs" }
func (fakeFileInfo) Size() int64        { return 0 }
func (fakeFileInfo) Mode() fs.FileMode  { return 0o750 }
func (fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (info fakeFileInfo) IsDir() bool   { return info.directory }
func (fakeFileInfo) Sys() any           { return nil }

func testDaemonAbsolutePath(elements ...string) string {
	if filepath.Separator == '\\' {
		return filepath.Join(append([]string{"C:" + string(filepath.Separator)}, elements...)...)
	}
	return filepath.Join(append([]string{string(filepath.Separator)}, elements...)...)
}
