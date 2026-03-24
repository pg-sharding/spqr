package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v2"
)

var (
	loadedConfigs = map[string]struct {
		cfg    any
		reload func() (any, error)
	}{}
)

type Config interface {
	ApplyDefaults()
	PostProcess() error
}

// cfg must be pointer
func initConfig(file *os.File, filepath string, cfg any) error {
	if strings.HasSuffix(filepath, ".toml") {
		_, err := toml.NewDecoder(file).Decode(cfg)
		return err
	}
	if strings.HasSuffix(filepath, ".yaml") {
		return yaml.NewDecoder(file).Decode(cfg)
	}
	if strings.HasSuffix(filepath, ".json") {
		return json.NewDecoder(file).Decode(cfg)
	}
	return fmt.Errorf("unknown config format type: %s. Use .toml, .yaml or .json suffix in filename", filepath)
}

func ParseCfg(cfgPath string, cfg any) error {
	file, err := os.Open(cfgPath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("failed to close config file: %v", err)
		}
	}(file)

	if err := initConfig(file, cfgPath, cfg); err != nil {
		return err
	}

	return nil
}

func LoadConfig(path string, cfg Config) (string, error) {
	load := func(cfg Config) error {
		cfg.ApplyDefaults()

		err := ParseCfg(path, cfg)
		if err != nil {
			return err
		}

		if err := cfg.PostProcess(); err != nil {
			return err
		}

		return nil
	}

	err := load(cfg)
	if err != nil {
		return "", err
	}

	configBytes, err := json.MarshalIndent(&cfg, "", "  ")
	if err != nil {
		return "", err
	}

	loadedConfigs[fmt.Sprintf("%s%s", path, reflect.TypeOf(cfg).String())] = struct {
		cfg    any
		reload func() (any, error)
	}{
		cfg: cfg,
		reload: func() (any, error) {
			newCfg := reflect.New(reflect.TypeOf(cfg).Elem()).Interface().(Config)
			err := load(newCfg)
			return newCfg, err
		},
	}

	return string(configBytes), nil
}

func ConfigChanges() ([]ConfigFieldState, error) {
	changes := make([]ConfigFieldState, 0)

	for _, d := range loadedConfigs {
		c, err := d.reload()
		if err != nil {
			return nil, err
		}
		cfgChanges, err := CompareConfigs(d.cfg, c)
		if err != nil {
			return nil, err
		}

		changes = append(changes, cfgChanges...)
	}

	return changes, nil
}

type ConfigFieldState struct {
	FieldName  string
	FieldValue string
	Applied    bool
}

func CompareConfigs(cfg1 any, cfg2 any) ([]ConfigFieldState, error) {
	t1 := reflect.TypeOf(cfg1)
	t2 := reflect.TypeOf(cfg2)

	if t1 != t2 {
		return nil, fmt.Errorf("configs have different types")
	}

	v1 := reflect.ValueOf(cfg1)
	v2 := reflect.ValueOf(cfg2)

	if v1.Kind() == reflect.Pointer {
		if v1.IsNil() {
			v1 = reflect.New(t1.Elem())
		}
		if v2.IsNil() {
			v2 = reflect.New(t1.Elem())
		}

		v1 = v1.Elem()
		v2 = v2.Elem()

		t1 = v1.Type()
	}

	if t1.Kind() != reflect.Struct {
		return nil, fmt.Errorf("config must be a struct")
	}

	state := make([]ConfigFieldState, 0)

	var compareValues func(prefix string, f1, f2 reflect.Value)

	compareValues = func(prefix string, f1, f2 reflect.Value) {
		if f1.Kind() == reflect.Pointer {
			if f1.IsNil() && f2.IsNil() {
				return
			}

			if !f1.IsNil() && f2.IsNil() {
				state = append(state, ConfigFieldState{
					FieldName:  prefix,
					FieldValue: "NULL",
					Applied:    false,
				})

				return
			}

			if f1.IsNil() {
				f1 = reflect.New(f1.Type().Elem())
			}

			f1 = f1.Elem()
			f2 = f2.Elem()
		}

		switch f1.Kind() {
		case reflect.Map:
			seen := map[any]struct{}{}

			for _, k := range f1.MapKeys() {
				seen[k.Interface()] = struct{}{}
			}
			for _, k := range f2.MapKeys() {
				seen[k.Interface()] = struct{}{}
			}

			for k := range seen {
				keyVal := reflect.ValueOf(k)

				v1Val := f1.MapIndex(keyVal)
				v2Val := f2.MapIndex(keyVal)

				path := fmt.Sprintf("%s.%v", prefix, k)

				if !v1Val.IsValid() {
					state = append(state, ConfigFieldState{
						FieldName:  path,
						FieldValue: "present",
						Applied:    false,
					})
					continue
				}
				if !v2Val.IsValid() {
					state = append(state, ConfigFieldState{
						FieldName:  path,
						FieldValue: "absent",
						Applied:    false,
					})
					continue
				}

				compareValues(path, v1Val, v2Val)
			}
		case reflect.Struct:
			t := f1.Type()
			for i := 0; i < f1.NumField(); i++ {
				if !t.Field(i).IsExported() {
					continue
				}

				fieldName := t.Field(i).Name
				fieldNameWithPrefix := fmt.Sprintf("%s.%s", prefix, fieldName)

				compareValues(fieldNameWithPrefix, f1.Field(i), f2.Field(i))
			}
		case reflect.Slice:
			for i := 0; i < min(f1.Len(), f2.Len()); i++ {
				pref := fmt.Sprintf("%s.%d", prefix, i)
				compareValues(pref, f1.Index(i), f2.Index(i))
			}
			for i := f1.Len(); i < f2.Len(); i++ {
				fieldName := fmt.Sprintf("%s.%d", prefix, i)
				state = append(state, ConfigFieldState{
					FieldName:  fieldName,
					FieldValue: "present",
					Applied:    false,
				})
			}
			for i := f2.Len(); i < f1.Len(); i++ {
				fieldName := fmt.Sprintf("%s.%d", prefix, i)
				state = append(state, ConfigFieldState{
					FieldName:  fieldName,
					FieldValue: "absent",
					Applied:    false,
				})
			}
		default:
			applied := reflect.DeepEqual(f1.Interface(), f2.Interface())
			state = append(state, ConfigFieldState{
				FieldName:  prefix,
				FieldValue: fmt.Sprintf("%v", f2.Interface()),
				Applied:    applied,
			})
		}
	}

	compareValues("", v1, v2)

	return state, nil
}
