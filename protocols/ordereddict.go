package protocols

import (
	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

// Implements ordereddict.Dict equality.
type _DictEq struct{}

func (self _DictEq) Eq(scope types.Scope, a types.Any, b types.Any) bool {
	a_dict, _ := to_dict(a)
	b_dict, _ := to_dict(b)

	if a_dict.Len() != b_dict.Len() {
		return false
	}

	for _, key := range a_dict.Keys() {
		a_value, pres := a_dict.Get(key)
		if !pres {
			return false
		}

		b_value, pres := b_dict.Get(key)
		if !pres {
			return false
		}

		if !scope.Eq(a_value, b_value) {
			return false
		}
	}

	return true
}

func to_dict(a types.Any) (*ordereddict.Dict, bool) {
	switch t := a.(type) {
	case ordereddict.Dict:
		return &t, true
	case *ordereddict.Dict:
		return t, true
	default:
		return nil, false
	}
}

func (self _DictEq) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := to_dict(a)
	_, b_ok := to_dict(b)

	return a_ok && b_ok
}
