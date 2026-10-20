package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func loadPrimaryCheckConfig(t *testing.T, format, content string) (*Router, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "router."+format)
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))

	previousRouter := cfgRouter
	t.Cleanup(func() {
		cfgRouter = previousRouter
		mu.Lock()
		delete(loadedConfigs, path+"*config.Router")
		mu.Unlock()
	})
	router := &Router{}
	_, err := LoadConfig(path, router)
	return router, err
}

func primaryCheckConfig(format, interval, timeout, concurrency string) string {
	if format == "json" {
		return fmt.Sprintf(`{"dbpool_primary_check_interval": %s, "dbpool_primary_check_timeout": %s, "dbpool_primary_check_concurrency": %s}`, interval, timeout, concurrency)
	}
	separator := ": "
	if format == "toml" {
		separator = " = "
	}
	return fmt.Sprintf("dbpool_primary_check_interval%s%q\ndbpool_primary_check_timeout%s%q\ndbpool_primary_check_concurrency%s%s\n", separator, interval, separator, timeout, separator, concurrency)
}

func TestPrimaryCheckConfigDefaults(t *testing.T) {
	for _, format := range []string{"yaml", "toml", "json"} {
		t.Run(format, func(t *testing.T) {
			content := "log_level = 'info'"
			switch format {
			case "yaml":
				content = "log_level: info"
			case "json":
				content = `{}`
			}
			cfg, err := loadPrimaryCheckConfig(t, format, content)
			require.NoError(t, err)
			require.Zero(t, cfg.DbpoolPrimaryCheckInterval)
			require.Equal(t, 2*time.Second, cfg.DbpoolPrimaryCheckTimeout)
			require.Equal(t, 4, cfg.DbpoolPrimaryCheckConcurrency)
		})
	}
}

func TestPrimaryCheckConfigValues(t *testing.T) {
	for _, format := range []string{"yaml", "toml", "json"} {
		t.Run(format, func(t *testing.T) {
			interval, timeout := "5s", "1500ms"
			if format == "json" {
				// Existing JSON duration fields are integer nanoseconds.
				interval, timeout = "5000000000", "1500000000"
			}
			cfg, err := loadPrimaryCheckConfig(t, format, primaryCheckConfig(format, interval, timeout, "7"))
			require.NoError(t, err)
			require.Equal(t, 5*time.Second, cfg.DbpoolPrimaryCheckInterval)
			require.Equal(t, 1500*time.Millisecond, cfg.DbpoolPrimaryCheckTimeout)
			require.Equal(t, 7, cfg.DbpoolPrimaryCheckConcurrency)
		})
	}
}

func TestPrimaryCheckConfigValidation(t *testing.T) {
	for _, format := range []string{"yaml", "toml", "json"} {
		t.Run(format, func(t *testing.T) {
			for _, tc := range []struct {
				name        string
				interval    string
				timeout     string
				concurrency string
				errorField  string
			}{
				{name: "disabled", interval: "0s", timeout: "2s", concurrency: "4"},
				{name: "negative interval", interval: "-1s", timeout: "2s", concurrency: "4", errorField: "dbpool_primary_check_interval"},
				{name: "zero timeout", interval: "5s", timeout: "0s", concurrency: "4", errorField: "dbpool_primary_check_timeout"},
				{name: "negative timeout", interval: "5s", timeout: "-1s", concurrency: "4", errorField: "dbpool_primary_check_timeout"},
				{name: "zero concurrency", interval: "5s", timeout: "2s", concurrency: "0", errorField: "dbpool_primary_check_concurrency"},
				{name: "negative concurrency", interval: "5s", timeout: "2s", concurrency: "-1", errorField: "dbpool_primary_check_concurrency"},
				{name: "disabled with invalid timeout", interval: "0s", timeout: "0s", concurrency: "4", errorField: "dbpool_primary_check_timeout"},
				{name: "disabled with invalid concurrency", interval: "0s", timeout: "2s", concurrency: "0", errorField: "dbpool_primary_check_concurrency"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					interval, timeout := tc.interval, tc.timeout
					if format == "json" {
						intervalDuration, err := time.ParseDuration(interval)
						require.NoError(t, err)
						timeoutDuration, err := time.ParseDuration(timeout)
						require.NoError(t, err)
						interval, timeout = fmt.Sprint(int64(intervalDuration)), fmt.Sprint(int64(timeoutDuration))
					}
					_, err := loadPrimaryCheckConfig(t, format, primaryCheckConfig(format, interval, timeout, tc.concurrency))
					if tc.errorField == "" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, tc.errorField)
					}
				})
			}
		})
	}
}
