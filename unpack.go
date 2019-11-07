package ebakusdb

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
)

// hasHexPrefix validates str begins with '0x' or '0X'.
func hasHexPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

func byteArrayToReflectValue(value []byte, t reflect.Type) (reflect.Value, error) {
	var (
		kind        = t.Kind()
		stringValue = string(value)
	)

	switch kind {
	case reflect.Bool:
		val, err := strconv.ParseBool(stringValue)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(val), nil
	case reflect.Int:
		val, err := strconv.ParseInt(stringValue, 0, 0)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int(val)), nil
	case reflect.Int8:
		val, err := strconv.ParseInt(stringValue, 0, 8)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int8(val)), nil
	case reflect.Int16:
		val, err := strconv.ParseInt(stringValue, 0, 16)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int16(val)), nil
	case reflect.Int32:
		val, err := strconv.ParseInt(stringValue, 0, 32)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int32(val)), nil
	case reflect.Int64:
		val, err := strconv.ParseInt(stringValue, 0, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(int64(val)), nil
	case reflect.Uint:
		val, err := strconv.ParseUint(stringValue, 0, 0)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint(val)), nil
	case reflect.Uint8:
		val, err := strconv.ParseUint(stringValue, 0, 8)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint8(val)), nil
	case reflect.Uint16:
		val, err := strconv.ParseUint(stringValue, 0, 16)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint16(val)), nil
	case reflect.Uint32:
		val, err := strconv.ParseUint(stringValue, 0, 32)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint32(val)), nil
	case reflect.Uint64:
		val, err := strconv.ParseUint(stringValue, 0, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(uint64(val)), nil
	case reflect.Float32:
		val, err := strconv.ParseFloat(stringValue, 32)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(float32(val)), nil
	case reflect.Float64:
		val, err := strconv.ParseFloat(stringValue, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(float64(val)), nil
	case reflect.String:
		return reflect.ValueOf(stringValue), nil
	case reflect.Slice, reflect.Array:
		if hasHexPrefix(stringValue) {
			decoded, err := hex.DecodeString(stringValue[2:])
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(decoded), nil
		}
	case reflect.Ptr:
		if t == reflect.TypeOf(&big.Int{}) {
			var val *big.Int
			if hasHexPrefix(stringValue) {
				decoded, err := hex.DecodeString(stringValue[2:])
				if err != nil {
					return reflect.Value{}, err
				}
				val = big.NewInt(0).SetBytes(decoded)
			} else {
				var ok bool
				val, ok = big.NewInt(0).SetString(stringValue, 10)
				if !ok {
					return reflect.Value{}, fmt.Errorf("unpack: failed to unpack big.Int")
				}
			}

			return reflect.ValueOf(val), nil
		}
	}

	return reflect.ValueOf(stringValue), nil
}
