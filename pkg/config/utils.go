package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v2"
)

// initConfig initializes the router configuration from a file.
//
// Parameters:
// - file: *os.File - the file to read the configuration from.
// - cfgRouter: *Router - a pointer to the router configuration struct.
//
// Returns:
// - error: an error if the configuration file format is unknown or if there was an error decoding the file.
func initConfig(file *os.File, target any) error {
	if strings.HasSuffix(file.Name(), ".toml") {
		_, err := toml.NewDecoder(file).Decode(target)
		return err
	}
	if strings.HasSuffix(file.Name(), ".yaml") {
		return yaml.NewDecoder(file).Decode(target)
	}
	if strings.HasSuffix(file.Name(), ".json") {
		return json.NewDecoder(file).Decode(target)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", file.Name())
}