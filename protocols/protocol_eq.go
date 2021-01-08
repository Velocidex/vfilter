package protocols

import (
	"reflect"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Eq protocol
type EqProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Eq(scope types.Scope, a types.Any, b types.Any) bool
}

type EqDispatcher struct {
	impl []EqProtocol
}

func (self EqDispatcher) Copy() EqDispatcher {
	return EqDispatcher{
		append([]EqProtocol{}, self.impl...)}
}

func (self EqDispatcher) Eq(scope types.Scope, a types.Any, b types.Any) bool {

	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return types.IsNullObject(b) // types.Null == types.Null else false

	case string:
		rhs, ok := b.(string)
		if ok {
			return t == rhs
		}

	case bool:
		rhs, ok := b.(bool)
		if ok {
			return t == rhs
		}

	case float64:
		rhs, ok := utils.ToFloat(b)
		if ok {
			return t == rhs
		}

	}

	lhs, ok := utils.ToInt64(a)
	if ok {
		rhs, ok := utils.ToInt64(b)
		if ok {
			return lhs == rhs
		}
	}

	if is_array(a) && is_array(b) {
		return _ArrayEq(scope, a, b)
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Eq(scope, a, b)
		}
	}

	scope.Trace("Protocol Equal not found for %v (%T) and %v (%T)",
		a, a, b, b)
	return false
}

func (self *EqDispatcher) AddImpl(elements ...EqProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

func _ArrayEq(scope types.Scope, a types.Any, b types.Any) bool {
	value_a := reflect.ValueOf(a)
	value_b := reflect.ValueOf(b)

	if value_a.Len() != value_b.Len() {
		return false
	}

	for i := 0; i < value_a.Len(); i++ {
		if !scope.Eq(value_a.Index(i).Interface(),
			value_b.Index(i).Interface()) {
			return false
		}
	}

	return true
}

func is_array(a types.Any) bool {
	rt := reflect.TypeOf(a)
	if rt == nil {
		return false
	}
	return rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array
}
