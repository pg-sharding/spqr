package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v2"
)

var cfgSequencer Sequencer

type Sequencer struct {
	LogLevel    string `json:"log_level" toml:"log_level" yaml:"log_level"`
	LogFileName string `json:"log_filename" toml:"log_filename" yaml:"log_filename"`

	StorageConnString string `json:"storage_connstring" toml:"storage_connstring" yaml:"storage_connstring"`

	QdbAddr string `json:"qdb_addr" toml:"qdb_addr" yaml:"qdb_addr"`
}

func initSequencerConfig(file *os.File, filepath string) error {
	if strings.HasSuffix(filepath, ".toml") {
		_, err := toml.NewDecoder(file).Decode(&cfgSequencer)
		return err
	}
	if strings.HasSuffix(filepath, ".yaml") {
		return yaml.NewDecoder(file).Decode(&cfgSequencer)
	}
	if strings.HasSuffix(filepath, ".json") {
		return json.NewDecoder(file).Decode(&cfgSequencer)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", filepath)
}

func LoadSequencerCfg(cfgPath string) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := initSequencerConfig(file, cfgPath); err != nil {
		return err
	}

	configBytes, err := json.MarshalIndent(&cfgSequencer, "", "  ")
	if err != nil {
		return err
	}

	log.Println("Running config:", string(configBytes))
	return nil
}

func SequencerConfig() *Sequencer {
	return &cfgSequencer
}
