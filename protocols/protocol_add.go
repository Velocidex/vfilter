package protocols

import (
	"reflect"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Add protocol
type AddProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Add(scope types.Scope, a types.Any, b types.Any) types.Any
}

type AddDispatcher struct {
	impl []AddProtocol
}

func (self AddDispatcher) Copy() AddDispatcher {
	return AddDispatcher{
		append([]AddProtocol{}, self.impl...)}
}

func (self AddDispatcher) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	switch t := a.(type) {
	case string:
		b_str, ok := b.(string)
		if ok {
			return t + b_str
		}
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		b_float, ok := utils.ToFloat(b)
		if ok {
			return t + b_float
		}
	}

	// Maybe its an integer.
	a_int, ok := utils.ToInt64(a)
	if ok {
		b_int, ok := utils.ToInt64(b)
		if ok {
			return a_int + b_int
		}
	}

	// Handle array concatenation
	if is_array(a) || is_array(b) {
		a_slice := convertToSlice(a)
		b_slice := convertToSlice(b)

		return append(a_slice, b_slice...)
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Add(scope, a, b)
		}
	}
	scope.Trace("Protocol Add not found for %v (%T) and %v (%T)",
		a, a, b, b)
	return types.Null{}
}

func (self *AddDispatcher) AddImpl(elements ...AddProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

func convertToSlice(a types.Any) []types.Any {
	if is_array(a) {
		a_slice := reflect.ValueOf(a)
		result := make([]types.Any, 0, a_slice.Len())
		for i := 0; i < a_slice.Len(); i++ {
			result = append(result, a_slice.Index(i).Interface())
		}
		return result
	}
	return []types.Any{a}
}
