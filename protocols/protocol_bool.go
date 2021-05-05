package protocols

import (
	"reflect"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
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

	case types.LazyExpr:
		return self.Bool(scope, t.Reduce())
	}

	if is_array(a) {
		value_a := reflect.ValueOf(a)
		return value_a.Len() > 0
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
