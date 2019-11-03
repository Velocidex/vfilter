package vfilter

import (
	"reflect"

	"github.com/Velocidex/ordereddict"
)

// Implements ordereddict.Dict equality.
type _DictEq struct{}

func (self _DictEq) Eq(scope *Scope, a Any, b Any) bool {
	return reflect.DeepEqual(a, b)
}

func to_dict(a Any) (*ordereddict.Dict, bool) {
	switch t := a.(type) {
	case ordereddict.Dict:
		return &t, true
	case *ordereddict.Dict:
		return t, true
	default:
		return nil, false
	}
}

func (self _DictEq) Applicable(a Any, b Any) bool {
	_, a_ok := to_dict(a)
	_, b_ok := to_dict(b)

	return a_ok && b_ok
}

type _DictAssociative struct{}

func (self _DictAssociative) Applicable(a Any, b Any) bool {
	_, a_ok := to_dict(a)
	_, b_ok := to_string(b)

	return a_ok && b_ok
}

// Associate object a with key b
func (self _DictAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	key, _ := to_string(b)
	value, _ := to_dict(a)

	res, pres := value.Get(key)
	if !pres {
		// Return the default value but still indicate the
		// value is not present.
		default_value := value.GetDefault()
		if default_value != nil {
			return default_value, false
		}
	}
	return res, pres
}

func (self _DictAssociative) GetMembers(scope *Scope, a Any) []string {
	value, ok := to_dict(a)
	if !ok {
		return nil
	}

	return value.Keys()
}

type _BoolDict struct{}

func (self _BoolDict) Applicable(a Any) bool {
	_, a_ok := to_dict(a)

	return a_ok
}

func (self _BoolDict) Bool(scope *Scope, a Any) bool {
	value, ok := to_dict(a)
	if !ok {
		return false
	}

	return value.Len() > 0
}
