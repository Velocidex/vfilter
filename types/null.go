package types

import "reflect"

// A real type which encodes to JSON NULL. Using go's nil is dangerous
// because it forces constant checking for nil pointer dereference. It
// is safer to just return this value when VQL needs to return NULL.
type Null struct{}

func (self Null) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

func (self Null) String() string {
	return "Null"
}

func IsNil(a interface{}) bool {
	if a == nil {
		return true
	}

	switch a.(type) {
	case Null, *Null:
		return true
	default:
		switch reflect.TypeOf(a).Kind() {
		case reflect.Ptr, reflect.Map, reflect.Chan, reflect.Slice:
			//use of IsNil method
			return reflect.ValueOf(a).IsNil()
		}
		return false
	}
}
