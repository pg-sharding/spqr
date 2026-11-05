package config

import (
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

var loggerCfgMu = sync.Mutex{}
var cfgLogger Logger

type Logger struct {
	DiodeSize    int           `json:"diode_size" toml:"diode_size" yaml:"diode_size"`
	PollInterval time.Duration `json:"poll_interval" toml:"poll_interval" yaml:"poll_interval"`
}

var _ Config = &Logger{}

func (c *Logger) ApplyDefaults() {
	c.DiodeSize = 1000
	c.PollInterval = 10 * time.Millisecond
}

func (c *Logger) PostProcess() error {
	if c.DiodeSize <= 0 {
		return spqrerror.New(spqrerror.SPQR_CONFIG_ERROR, "diode size must be posistive")
	}
	return nil
}

// LoadCoordinatorCfg loads the coordinator configuration from the specified file path.
//
// Parameters:
//   - cfgPath (string): The path of the configuration file.
//
// Returns:
//   - string: JSON-formatted config
//   - error: An error if any occurred during the loading process.
func LoadLoggerCfg(cfgPath string) (string, error) {
	c := &Logger{}
	configStr, err := LoadConfig(cfgPath, c)
	if err != nil {
		return "", err
	}

	loggerCfgMu.Lock()
	defer loggerCfgMu.Unlock()
	cfgLogger = *c
	return configStr, nil
}

// CoordinatorConfig returns a pointer to the Coordinator configuration.
//
// Returns:
//   - *Coordinator: a pointer to the Coordinator configuration.
func LoggerConfig() *Logger {
	return &cfgLogger
}
