package ebakusdb

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
)

// hasHexPrefix validates str begins with '0x' or '0X'.
func hasHexPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

func stringToReflectValue(value string, t reflect.Type) (reflect.Value, error) {
	kind := t.Kind()

	if t.Kind() == reflect.Ptr && t == reflect.TypeOf(&big.Int{}) {
		var val *big.Int
		if hasHexPrefix(value) {
			decoded, err := hex.DecodeString(value[2:])
			if err != nil {
				return reflect.Value{}, err
			}
			val = big.NewInt(0).SetBytes(decoded)
		} else {
			var ok bool
			val, ok = big.NewInt(0).SetString(value, 10)
			if !ok {
				return reflect.Value{}, fmt.Errorf("unpack: failed to unpack big.Int")
			}
		}

		return reflect.ValueOf(val), nil
	}

	switch kind {
	case reflect.Bool:
		value, err := strconv.ParseBool(value)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(value), nil
	case reflect.Int:
		value, err := strconv.ParseInt(value, 0, 0)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int(value)), nil
	case reflect.Int8:
		value, err := strconv.ParseInt(value, 0, 8)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int8(value)), nil
	case reflect.Int16:
		value, err := strconv.ParseInt(value, 0, 16)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int16(value)), nil
	case reflect.Int32:
		value, err := strconv.ParseInt(value, 0, 32)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int32(value)), nil
	case reflect.Int64:
		value, err := strconv.ParseInt(value, 0, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int64(value)), nil
	case reflect.Uint:
		value, err := strconv.ParseUint(value, 0, 0)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint(value)), nil
	case reflect.Uint8:
		value, err := strconv.ParseUint(value, 0, 8)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint8(value)), nil
	case reflect.Uint16:
		value, err := strconv.ParseUint(value, 0, 16)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint16(value)), nil
	case reflect.Uint32:
		value, err := strconv.ParseUint(value, 0, 32)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint32(value)), nil
	case reflect.Uint64:
		value, err := strconv.ParseUint(value, 0, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint64(value)), nil
	case reflect.Float32:
		value, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(float32(value)), nil
	case reflect.Float64:
		value, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(float64(value)), nil
	case reflect.String:
		return reflect.ValueOf(value), nil
	case reflect.Slice, reflect.Array:
		if hasHexPrefix(value) {
			decoded, err := hex.DecodeString(value[2:])
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(decoded), nil
		}
	}

	return reflect.ValueOf(value), nil
}
