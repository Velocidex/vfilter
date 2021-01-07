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

type _AddStrings struct{}

func (self _AddStrings) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToString(a)
	_, b_ok := utils.ToString(b)
	return a_ok && b_ok
}

func (self _AddStrings) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_str, _ := utils.ToString(a)
	b_str, _ := utils.ToString(b)

	return a_str + b_str
}

type _AddInts struct{}

func (self _AddInts) Applicable(a types.Any, b types.Any) bool {
	return utils.IsInt(a) && utils.IsInt(b)
}

func (self _AddInts) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToInt64(a)
	b_val, _ := utils.ToInt64(b)
	return a_val + b_val
}

type _AddFloats struct{}

func (self _AddFloats) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToFloat(a)
	_, b_ok := utils.ToFloat(b)
	return a_ok && b_ok
}

func (self _AddFloats) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToFloat(a)
	b_val, _ := utils.ToFloat(b)
	return a_val + b_val
}

// Add two slices together.
type _AddSlices struct{}

func (self _AddSlices) Applicable(a types.Any, b types.Any) bool {
	return is_array(a) && is_array(b)
}

func (self _AddSlices) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	var result []types.Any
	a_slice := reflect.ValueOf(a)
	b_slice := reflect.ValueOf(b)

	for i := 0; i < a_slice.Len(); i++ {
		result = append(result, a_slice.Index(i).Interface())
	}

	for i := 0; i < b_slice.Len(); i++ {
		result = append(result, b_slice.Index(i).Interface())
	}

	return result
}

func is_null(a types.Any) bool {
	if a == nil {
		return true
	}

	switch a.(type) {
	case types.Null, *types.Null:
		return true
	}

	return false
}

// Add a slice to null. We treat null as the empty array.
type _AddNull struct{}

func (self _AddNull) Applicable(a types.Any, b types.Any) bool {
	return (is_array(a) && is_null(b)) || (is_null(a) && is_array(b))
}

func (self _AddNull) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	if is_null(a) {
		return b
	}
	return a
}

// Add a slice to types.Any will expand the slice and add each item with the
// any.
type _AddSliceAny struct{}

func (self _AddSliceAny) Applicable(a types.Any, b types.Any) bool {
	return is_array(a) || is_array(b)
}

func (self _AddSliceAny) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	var result []types.Any

	if is_array(a) {
		a_slice := reflect.ValueOf(a)

		for i := 0; i < a_slice.Len(); i++ {
			result = append(result, a_slice.Index(i).Interface())
		}

		return append(result, b)
	}

	result = append(result, a)
	b_slice := reflect.ValueOf(b)

	for i := 0; i < b_slice.Len(); i++ {
		result = append(result, b_slice.Index(i).Interface())
	}

	return result
}
