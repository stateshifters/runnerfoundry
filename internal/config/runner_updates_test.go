package config

import (
	"fmt"
	"strings"
	"testing"
)

func TestLoadDefaultsRunnerAutoUpdatesToDisabled(t *testing.T) {
	loadedConfig := loadValidConfig(t)
	if loadedConfig.AutoUpdateRunners {
		t.Fatal("runner auto-updates were enabled by default")
	}
}

func TestLoadAcceptsRunnerAutoUpdateSetting(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			configText := fmt.Sprintf("autoUpdateRunners: %t\n%s", enabled, validConfig(privateKeyPath))
			loadedConfig, err := Load(strings.NewReader(configText))
			if err != nil {
				t.Fatalf("load runner auto-update setting: %v", err)
			}
			if loadedConfig.AutoUpdateRunners != enabled {
				t.Fatalf("AutoUpdateRunners = %t, want %t", loadedConfig.AutoUpdateRunners, enabled)
			}
		})
	}
}
