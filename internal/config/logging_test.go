package config

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogFileConfigurationParsesFromYAML(t *testing.T) {
	configurationYAML := fmt.Sprintf(`
github:
  app:
    app_id: 1
    private_key_file: mocked.pem
log:
  format: json
  level: debug
  file:
    path: %q
    max_size_mb: 25
    max_backups: 3
state:
  path: mocked-state.db
scale_sets:
  - name: linux
    max_runners: 1
    docker:
      compose_file: mocked-compose.yaml
      service: runner
`, testAbsoluteLogPath())
	configured, err := Load(strings.NewReader(configurationYAML))
	if err != nil {
		t.Fatalf("parse file logging configuration: %v", err)
	}
	if configured.Log.File == nil || *configured.Log.File.MaxSizeMB != 25 || *configured.Log.File.MaxBackups != 3 {
		t.Fatalf("parsed file logging configuration = %#v", configured.Log.File)
	}
}

func TestLogFileConfigurationAppliesBoundedRotationDefaults(t *testing.T) {
	configured := validInMemoryConfiguration()
	configured.Log.File = &LogFileConfig{Path: testAbsoluteLogPath()}

	if err := configured.Validate(); err != nil {
		t.Fatalf("validate file logging: %v", err)
	}
	if *configured.Log.File.MaxSizeMB != 100 || *configured.Log.File.MaxBackups != 5 {
		t.Fatalf("file logging defaults = %#v", configured.Log.File)
	}
}

func TestLogFileConfigurationRejectsUnsafeValues(t *testing.T) {
	absolutePath := testAbsoluteLogPath()
	tests := map[string]LogFileConfig{
		"relative path":    {Path: "runnerfoundry.log", MaxSizeMB: testIntPointer(1), MaxBackups: testIntPointer(1)},
		"empty path":       {MaxSizeMB: testIntPointer(1), MaxBackups: testIntPointer(1)},
		"zero size":        {Path: absolutePath, MaxSizeMB: testIntPointer(0), MaxBackups: testIntPointer(1)},
		"negative size":    {Path: absolutePath, MaxSizeMB: testIntPointer(-1), MaxBackups: testIntPointer(1)},
		"zero backups":     {Path: absolutePath, MaxSizeMB: testIntPointer(1), MaxBackups: testIntPointer(0)},
		"negative backups": {Path: absolutePath, MaxSizeMB: testIntPointer(1), MaxBackups: testIntPointer(-1)},
		"size overflow":    {Path: absolutePath, MaxSizeMB: testIntPointer(math.MaxInt), MaxBackups: testIntPointer(1)},
	}

	for testName, fileConfig := range tests {
		t.Run(testName, func(t *testing.T) {
			configured := validInMemoryConfiguration()
			configured.Log.File = &fileConfig
			if err := configured.Validate(); err == nil {
				t.Fatal("expected invalid file logging configuration")
			}
		})
	}
}

func testIntPointer(value int) *int {
	return &value
}

func testAbsoluteLogPath() string {
	if filepath.Separator == '\\' {
		return filepath.Join("C:"+string(filepath.Separator), "var", "log", "runnerfoundry.log")
	}
	return filepath.Join(string(filepath.Separator), "var", "log", "runnerfoundry.log")
}

func TestLogConfigurationWithoutFilePreservesStandardOutput(t *testing.T) {
	configured := validInMemoryConfiguration()
	if err := configured.Validate(); err != nil {
		t.Fatalf("validate stdout logging: %v", err)
	}
	if configured.Log.File != nil {
		t.Fatalf("unexpected file logging configuration %#v", configured.Log.File)
	}
}

func validInMemoryConfiguration() Config {
	return Config{
		GitHub: GitHubConfig{App: GitHubAppConfig{AppID: 1, PrivateKeyFile: "private-key.pem"}},
		Log:    LogConfig{Format: "text", Level: "info"},
		State:  StateConfig{Path: "state.db"},
		ScaleSets: []ScaleSetConfig{{
			Name: "linux", MaxRunners: 1,
			Docker: &DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
		}},
	}
}
