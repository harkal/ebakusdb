package ebakusdb

import (
	"encoding/json"
	"reflect"
)

// toGoType decodes arbitary bytes using JSON unmarshal, while maintaing the correct number type
// for int/uint types.
func toGoType(kind reflect.Kind, input []byte) (interface{}, error) {
	switch kind {
	case reflect.Bool:
		var out bool
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Int:
		var out int
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Int8:
		var out int8
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Int16:
		var out int16
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Int32:
		var out int32
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Int64:
		var out int64
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Uint:
		var out uint
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Uint8:
		var out uint8
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Uint16:
		var out uint16
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Uint32:
		var out uint32
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Uint64:
		var out uint64
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Float32:
		var out float32
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Float64:
		var out float64
		err := json.Unmarshal(input, &out)
		return out, err
	case reflect.Slice, reflect.Array:
		return input, nil
	}
	var out interface{}
	err := json.Unmarshal(input, &out)
	return out, err
}
