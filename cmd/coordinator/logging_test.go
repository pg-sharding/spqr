package main

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/spf13/cobra"
)

func TestPrettyLoggingConfigAndFlagPriority(t *testing.T) {
	tests := []struct {
		name           string
		configValue    bool
		flagPassed     bool
		flagValue      bool
		expectedResult bool
	}{
		{"DefaultNoConfigNoFlag", false, false, false, false},
		{"ConfigTrueNoFlag", true, false, false, true},
		{"ConfigFalseFlagTrue", false, true, true, true},
		{"ConfigTrueFlagFalse", true, true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Coordinator{
				PrettyLogging: tt.configValue,
			}

			cmd := &cobra.Command{}
			var prettyLog bool
			cmd.Flags().BoolVarP(&prettyLog, "pretty-log", "P", false, "")

			if tt.flagPassed {
				value := "false"
				if tt.flagValue {
					value = "true"
				}
				if err := cmd.Flags().Set("pretty-log", value); err != nil {
					t.Fatalf("failed to set flag: %v", err)
				}
				prettyLog = tt.flagValue
			}

			if cmd.Flags().Changed("pretty-log") {
				cfg.PrettyLogging = prettyLog
			}

			if cfg.PrettyLogging != tt.expectedResult {
				t.Fatalf("expected PrettyLogging=%v, got %v", tt.expectedResult, cfg.PrettyLogging)
			}
		})
	}
}
