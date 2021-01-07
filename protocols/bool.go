package protocols

import (
	"reflect"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type BoolDispatcher struct {
	impl []BoolProtocol
}

func (self BoolDispatcher) Copy() BoolDispatcher {
	return BoolDispatcher{
		append([]BoolProtocol{}, self.impl...)}
}

func (self BoolDispatcher) Bool(scope types.Scope, a types.Any) bool {

	// Handle directly the built in types for speed.
	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return false
	case bool:
		return t
	case int:
		return t > 0
	case int8:
		return t > 0
	case int16:
		return t > 0
	case int32:
		return t > 0
	case int64:
		return t > 0
	case uint8:
		return t > 0
	case uint16:
		return t > 0
	case uint32:
		return t > 0
	case uint64:
		return t > 0
	case float64:
		return t > 0

	case string:
		return len(t) > 0
	case *string:
		return len(*t) > 0

	case *ordereddict.Dict:
		return t.Len() > 0
	}

	for i, impl := range self.impl {
		if impl.Applicable(a) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Bool(scope, a)
		}
	}

	scope.Trace("Protocol Bool not found for %v (%T)", a, a)
	return false
}

func (self *BoolDispatcher) AddImpl(elements ...BoolProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

// This protocol implements the truth value.
type BoolProtocol interface {
	Applicable(a types.Any) bool
	Bool(scope types.Scope, a types.Any) bool
}

// Bool Implementations
type _BoolImpl struct{}

func (self _BoolImpl) Bool(scope types.Scope, a types.Any) bool {
	return a.(bool)
}

func (self _BoolImpl) Applicable(a types.Any) bool {
	_, ok := a.(bool)
	return ok
}

type _BoolInt struct{}

func (self _BoolInt) Bool(scope types.Scope, a types.Any) bool {
	a_val, _ := utils.ToFloat(a)
	if a_val != 0 {
		return true
	}

	return false
}

func (self _BoolInt) Applicable(a types.Any) bool {
	_, a_ok := utils.ToFloat(a)
	return a_ok
}

type _BoolString struct{}

func (self _BoolString) Bool(scope types.Scope, a types.Any) bool {
	switch t := a.(type) {
	case string:
		return len(t) > 0
	case *string:
		return len(*t) > 0
	}
	return false
}

func (self _BoolString) Applicable(a types.Any) bool {
	switch a.(type) {
	case string, *string:
		return true
	}
	return false
}

type _BoolSlice struct{}

func (self _BoolSlice) Applicable(a types.Any) bool {
	return is_array(a)
}

func (self _BoolSlice) Bool(scope types.Scope, a types.Any) bool {
	value_a := reflect.ValueOf(a)
	return value_a.Len() > 0
}
