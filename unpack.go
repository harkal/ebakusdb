package ebakusdb

import (
	"encoding/json"
	"reflect"
)

// toGoType decodes arbitary bytes using JSON unmarshal, while maintaing the correct number type
// for int/uint types.
func toGoType(kind reflect.Kind, input []byte) interface{} {
	switch kind {
	case reflect.Bool:
		var out bool
		json.Unmarshal(input, &out)
		return out
	case reflect.Int:
		var out int
		json.Unmarshal(input, &out)
		return out
	case reflect.Int8:
		var out int8
		json.Unmarshal(input, &out)
		return out
	case reflect.Int16:
		var out int16
		json.Unmarshal(input, &out)
		return out
	case reflect.Int32:
		var out int32
		json.Unmarshal(input, &out)
		return out
	case reflect.Int64:
		var out int64
		json.Unmarshal(input, &out)
		return out
	case reflect.Uint:
		var out uint
		json.Unmarshal(input, &out)
		return out
	case reflect.Uint8:
		var out uint8
		json.Unmarshal(input, &out)
		return out
	case reflect.Uint16:
		var out uint16
		json.Unmarshal(input, &out)
		return out
	case reflect.Uint32:
		var out uint32
		json.Unmarshal(input, &out)
		return out
	case reflect.Uint64:
		var out uint64
		json.Unmarshal(input, &out)
		return out
	case reflect.Float32:
		var out float32
		json.Unmarshal(input, &out)
		return out
	case reflect.Float64:
		var out float64
		json.Unmarshal(input, &out)
		return out
	}
	var out interface{}
	json.Unmarshal(input, &out)
	return out
}
