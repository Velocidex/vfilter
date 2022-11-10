package utils

import (
	"reflect"
	"unicode"

	"github.com/alecthomas/repr"
)

func Debug(arg interface{}) {
	if arg != nil {
		repr.Println(arg)
	} else {
		repr.Println("nil")
	}
}

// Is the symbol exported by Go? Only names with upper case are exported.
func IsExported(name string) bool {
	switch name {
	// Ignore common methods which should not be exported.
	case "MarshalJSON", "MarshalYAML":
		return false

	default:
		if len(name) == 0 || name[0] == '_' {
			return false
		}

		runes := []rune(name)
		return runes[0] == unicode.ToUpper(runes[0])
	}
}

func IsCallable(method_value reflect.Value, field_name string) bool {
	if !method_value.IsValid() {
		return false
	}

	// The name must be exportable.
	if !IsExported(field_name) {
		return false
	}

	// The function must have no args.
	if method_value.Type().NumIn() != 0 {
		return false
	}

	return true
}

func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}

	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Chan, reflect.Slice:
		//use of IsNil method
		return reflect.ValueOf(i).IsNil()
	}
	return false
}

func InString(hay *[]string, needle string) bool {
	for _, x := range *hay {
		if x == needle {
			return true
		}
	}

	return false
}

func IsArray(a interface{}) bool {
	rt := reflect.TypeOf(a)
	if rt == nil {
		return false
	}
	return rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array
}

// Try very hard to convert to a string
func ToString(x interface{}) (string, bool) {
	switch t := x.(type) {
	case string:
		return t, true
	case *string:
		return *t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

func ToFloat(x interface{}) (float64, bool) {
	switch t := x.(type) {
	case bool:
		if t {
			return 1, true
		} else {
			return 0, true
		}
	case float64:
		return t, true
	case int:
		return float64(t), true
	case uint:
		return float64(t), true

	case int8:
		return float64(t), true
	case int16:
		return float64(t), true
	case uint8:
		return float64(t), true
	case uint16:
		return float64(t), true

	case uint32:
		return float64(t), true
	case int32:
		return float64(t), true
	case uint64:
		return float64(t), true
	case int64:
		return float64(t), true
	case *float64:
		return *t, true
	case *int:
		return float64(*t), true
	case *uint:
		return float64(*t), true

	case *int8:
		return float64(*t), true
	case *int16:
		return float64(*t), true
	case *uint8:
		return float64(*t), true
	case *uint16:
		return float64(*t), true

	case *uint32:
		return float64(*t), true
	case *int32:
		return float64(*t), true
	case *uint64:
		return float64(*t), true
	case *int64:
		return float64(*t), true

	default:
		return 0, false
	}
}

// Does x resemble a int?
func IsInt(x interface{}) bool {
	switch x.(type) {
	case bool, int, int8, int16, int32, int64,
		uint8, uint16, uint32, uint64:
		return true
	}

	return false
}

func ToInt64(x interface{}) (int64, bool) {
	switch t := x.(type) {
	case bool:
		if t {
			return 1, true
		} else {
			return 0, true
		}
	case int:
		return int64(t), true
	case uint8:
		return int64(t), true
	case int8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case int16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case int32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true

	case *int:
		return int64(*t), true
	case *uint8:
		return int64(*t), true
	case *int8:
		return int64(*t), true
	case *uint16:
		return int64(*t), true
	case *int16:
		return int64(*t), true
	case *uint32:
		return int64(*t), true
	case *int32:
		return int64(*t), true
	case *uint64:
		return int64(*t), true
	case *int64:
		return int64(*t), true
	case *float64:
		return int64(*t), true

	default:
		return 0, false
	}
}
