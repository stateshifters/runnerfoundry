package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadRunnerPoolStartTimeouts(t *testing.T) {
	for _, test := range []struct {
		name    string
		setting string
		want    time.Duration
	}{
		{name: "omitted", want: 5 * time.Minute},
		{name: "custom", setting: "12m30s", want: 12*time.Minute + 30*time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			configured, err := Load(strings.NewReader(configWithStartTimeout(test.setting)))
			if err != nil {
				t.Fatal(err)
			}
			duration, err := configured.RunnerPools[0].StopIfNotStartedDuration()
			if err != nil || duration != test.want {
				t.Fatalf("start timeout = %v, %v; want %v", duration, err, test.want)
			}
		})
	}
}

func TestLoadRejectsInvalidRunnerPoolStartTimeouts(t *testing.T) {
	for _, setting := range []string{"later", "0s", "-1m", "999999999999h"} {
		t.Run(setting, func(t *testing.T) {
			_, err := Load(strings.NewReader(configWithStartTimeout(setting)))
			if err == nil || !strings.Contains(err.Error(), "stop_if_not_started_within") {
				t.Fatalf("Load() error = %v, want setting-specific error", err)
			}
		})
	}
}

func configWithStartTimeout(setting string) string {
	document := validConfig("app.pem")
	if setting == "" {
		return document
	}
	return strings.Replace(document, "    max_runners: 1", "    stop_if_not_started_within: "+setting+"\n    max_runners: 1", 1)
}
