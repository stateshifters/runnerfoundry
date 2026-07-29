//go:build integration

package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
)

func TestProductionLogDestinationWritesToConfiguredRotatingFile(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "runnerfoundry.log")
	maxSize := 1
	maxBackups := 1
	destination, err := newProductionLogDestinationFactory().Open(config.LogConfig{File: &config.LogFileConfig{
		Path: logPath, MaxSizeMB: &maxSize, MaxBackups: &maxBackups,
	}})
	if err != nil {
		t.Fatalf("open production log destination: %v", err)
	}
	if _, err = destination.Write([]byte("production log boundary")); err != nil {
		t.Fatalf("write production log destination: %v", err)
	}
	if err = destination.Close(); err != nil {
		t.Fatalf("close production log destination: %v", err)
	}
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read production log destination: %v", err)
	}
	if !strings.Contains(string(contents), "production log boundary") {
		t.Fatalf("log contents = %q", contents)
	}
}
