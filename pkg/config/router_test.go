package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterShowHintsConfig(t *testing.T) {
	for _, format := range []string{"yaml", "json", "toml"} {
		for _, tc := range []struct {
			name  string
			value string
			want  bool
		}{
			{name: "default", want: true},
			{name: "enabled", value: "true", want: true},
			{name: "disabled", value: "false", want: false},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				var content string
				switch format {
				case "yaml":
					content = "{}"
					if tc.value != "" {
						content = "show_hints: " + tc.value
					}
				case "json":
					content = "{}"
					if tc.value != "" {
						content = `{"show_hints": ` + tc.value + "}"
					}
				case "toml":
					if tc.value != "" {
						content = "show_hints = " + tc.value
					}
				}

				path := filepath.Join(t.TempDir(), "router."+format)
				require.NoError(t, os.WriteFile(path, []byte(content), 0600))
				cfg := &Router{}
				// LoadConfig registers the config for reload; do not retain a temporary file.
				t.Cleanup(func() {
					mu.Lock()
					defer mu.Unlock()
					for key, loaded := range loadedConfigs {
						if loaded.cfg == cfg {
							delete(loadedConfigs, key)
						}
					}
				})
				_, err := LoadConfig(path, cfg)
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.ShowHints)
			})
		}
	}
}
