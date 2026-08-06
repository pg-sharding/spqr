package config

import (
	"encoding/json"
	"reflect"
)

const RedactedValue = "***REDACTED***"

// MarshalRedactedJSON marshals v to JSON after replacing string fields tagged
// with secret:"true". It operates on a copy and does not mutate v.
func MarshalRedactedJSON(v any) ([]byte, error) {
	return json.Marshal(redactedValue(v))
}

func marshalRedactedJSONIndent(v any, prefix, indent string) ([]byte, error) {
	return json.MarshalIndent(redactedValue(v), prefix, indent)
}

func redactedValue(v any) any {
	value := reflect.ValueOf(v)
	if !value.IsValid() {
		return nil
	}
	return redactSecrets(value).Interface()
}

func redactSecrets(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(redactSecrets(value.Elem()))
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		valueType := value.Type()
		for i := 0; i < value.NumField(); i++ {
			fieldType := valueType.Field(i)
			if fieldType.Tag.Get("secret") == "true" {
				result.Field(i).SetString(RedactedValue)
			} else if fieldType.IsExported() {
				result.Field(i).Set(redactSecrets(value.Field(i)))
			}
		}
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			result.SetMapIndex(iterator.Key(), redactSecrets(iterator.Value()))
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(redactSecrets(value.Index(i)))
		}
		return result
	default:
		return value
	}
}
