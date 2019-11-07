package ebakusdb

import (
	"errors"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
)

// Comparison.
var (
	errBadComparisonType = errors.New("invalid type for comparison")
	errBadComparison     = errors.New("incompatible types for comparison")
	errNoComparison      = errors.New("missing argument for comparison")
)

type kind int

const (
	invalidKind kind = iota
	boolKind
	complexKind
	intKind
	floatKind
	stringKind
	uintKind
	arrayKind
	sliceKind
	addressKind
	bigIntKind
)

type ComparisonFunction = func(arg1, arg2 reflect.Value) (bool, error)

// indirectInterface returns the concrete value in an interface value,
// or else the zero reflect.Value.
// That is, if v represents the interface value x, the result is the same as reflect.ValueOf(x):
// the fact that x was an interface value is forgotten.
func indirectInterface(v reflect.Value) reflect.Value {
	if v.Kind() != reflect.Interface {
		return v
	}
	if v.IsNil() {
		return reflect.Value{}
	}
	return v.Elem()
}

func basicKind(v reflect.Value) (kind, error) {
	var (
		kind = v.Kind()
		typ  = v.Type()
	)
	switch kind {
	case reflect.Bool:
		return boolKind, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intKind, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintKind, nil
	case reflect.Float32, reflect.Float64:
		return floatKind, nil
	case reflect.Complex64, reflect.Complex128:
		return complexKind, nil
	case reflect.String:
		return stringKind, nil
	case reflect.Array:
		if typ == reflect.TypeOf(common.Address{}) {
			return addressKind, nil
		}
		return arrayKind, nil
	case reflect.Slice:
		return sliceKind, nil
	case reflect.Ptr:
		if typ == reflect.TypeOf(&big.Int{}) {
			return bigIntKind, nil
		}
	}
	return invalidKind, errBadComparisonType
}

func sliceToArray(s reflect.Value) reflect.Value {
	t := reflect.ArrayOf(s.Len(), s.Type().Elem())
	a := reflect.New(t).Elem()
	reflect.Copy(a, s)
	return a
}

// eq evaluates the comparison a == b
func eq(arg1, arg2 reflect.Value) (bool, error) {
	return eqM(arg1, arg2)
}

// eqM evaluates the comparison a == b || a == c || ...
func eqM(arg1 reflect.Value, arg2 ...reflect.Value) (bool, error) {
	v1 := indirectInterface(arg1)
	k1, err := basicKind(v1)
	if err != nil {
		return false, err
	}
	if len(arg2) == 0 {
		return false, errNoComparison
	}
	for _, arg := range arg2 {
		v2 := indirectInterface(arg)
		k2, err := basicKind(v2)
		if err != nil {
			return false, err
		}
		truth := false
		if k1 != k2 {
			// Special case: Can compare integer values regardless of type's sign.
			switch {
			case k1 == intKind && k2 == uintKind:
				truth = v1.Int() >= 0 && uint64(v1.Int()) == v2.Uint()
			case k1 == uintKind && k2 == intKind:
				truth = v2.Int() >= 0 && v1.Uint() == uint64(v2.Int())
			case (k1 == arrayKind && k2 == sliceKind) || (k1 == sliceKind && k2 == arrayKind):
				truth = reflect.DeepEqual(sliceToArray(v1).Interface(), sliceToArray(v2).Interface())
			default:
				return false, errBadComparison
			}
		} else {
			switch k1 {
			case boolKind:
				truth = v1.Bool() == v2.Bool()
			case complexKind:
				truth = v1.Complex() == v2.Complex()
			case floatKind:
				truth = v1.Float() == v2.Float()
			case intKind:
				truth = v1.Int() == v2.Int()
			case stringKind:
				truth = v1.String() == v2.String()
			case uintKind:
				truth = v1.Uint() == v2.Uint()
			case arrayKind, sliceKind:
				truth = reflect.DeepEqual(v1, v2)
			case addressKind:
				v1Addr := v1.Interface().(common.Address)
				v2Addr := v2.Interface().(common.Address)
				truth = v1Addr == v2Addr
			case bigIntKind:
				v1Big := v1.Interface().(*big.Int)
				v2Big := v2.Interface().(*big.Int)
				if v1Big == nil {
					v1Big = big.NewInt(0)
				}
				if v2Big == nil {
					v2Big = big.NewInt(0)
				}
				truth = v1Big.Cmp(v2Big) == 0
			default:
				return false, errBadComparisonType
			}
		}
		if truth {
			return true, nil
		}
	}
	return false, nil
}

// ne evaluates the comparison a != b.
func ne(arg1, arg2 reflect.Value) (bool, error) {
	// != is the inverse of ==.
	equal, err := eq(arg1, arg2)
	return !equal, err
}

// lt evaluates the comparison a < b.
func lt(arg1, arg2 reflect.Value) (bool, error) {
	v1 := indirectInterface(arg1)
	k1, err := basicKind(v1)
	if err != nil {
		return false, err
	}
	v2 := indirectInterface(arg2)
	k2, err := basicKind(v2)
	if err != nil {
		return false, err
	}
	truth := false
	if k1 != k2 {
		// Special case: Can compare integer values regardless of type's sign.
		switch {
		case k1 == intKind && k2 == uintKind:
			truth = v1.Int() < 0 || uint64(v1.Int()) < v2.Uint()
		case k1 == uintKind && k2 == intKind:
			truth = v2.Int() >= 0 && v1.Uint() < uint64(v2.Int())
		default:
			return false, errBadComparison
		}
	} else {
		switch k1 {
		case boolKind, complexKind:
			return false, errBadComparisonType
		case floatKind:
			truth = v1.Float() < v2.Float()
		case intKind:
			truth = v1.Int() < v2.Int()
		case stringKind:
			truth = v1.String() < v2.String()
		case uintKind:
			truth = v1.Uint() < v2.Uint()
		case bigIntKind:
			v1Big := v1.Interface().(*big.Int)
			v2Big := v2.Interface().(*big.Int)
			if v1Big == nil {
				v1Big = big.NewInt(0)
			}
			if v2Big == nil {
				v2Big = big.NewInt(0)
			}
			truth = v1Big.Cmp(v2Big) < 0
		default:
			return false, errBadComparisonType
		}
	}
	return truth, nil
}

// le evaluates the comparison <= b.
func le(arg1, arg2 reflect.Value) (bool, error) {
	// <= is < or ==.
	lessThan, err := lt(arg1, arg2)
	if lessThan || err != nil {
		return lessThan, err
	}
	return eq(arg1, arg2)
}

// gt evaluates the comparison a > b.
func gt(arg1, arg2 reflect.Value) (bool, error) {
	// > is the inverse of <=.
	lessOrEqual, err := le(arg1, arg2)
	if err != nil {
		return false, err
	}
	return !lessOrEqual, nil
}

// ge evaluates the comparison a >= b.
func ge(arg1, arg2 reflect.Value) (bool, error) {
	// >= is the inverse of <.
	lessThan, err := lt(arg1, arg2)
	if err != nil {
		return false, err
	}
	return !lessThan, nil
}
