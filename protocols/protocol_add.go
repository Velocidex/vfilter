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

// Adding protocol

// LHS    RHS
// int    int  -> lhs + rhs
// int    float -> float(lhs) + rhs
// float  int -> lhs + float(rhs)
// float  float -> lhs + rhs

// We dont handle any other additions with ints here.
func intAdd(lhs int64, b types.Any) (types.Any, bool) {
	switch b.(type) {
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		rhs, _ := utils.ToInt64(b)
		return lhs + rhs, true

	case float64, float32:
		rhs, _ := utils.ToFloat(b)
		return float64(lhs) + rhs, true
	}

	// We dont handle any other additions here
	return &types.Null{}, false
}

func (self AddDispatcher) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	a = maybeReduce(a)
	b = maybeReduce(b)

	switch t := a.(type) {
	case string:
		b_str, ok := b.(string)
		if ok {
			// Estimate how much memory we will use when adding the
			// string
			memory := len(t) * len(b_str)
			if memory > 100000000 { // 100mb
				scope.Log("Multiply Str x Int exceeded memory limits")
				return &types.Null{}
			}

			return t + b_str
		}

	case types.Null, *types.Null, nil:
		return &types.Null{}

	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		lhs, ok := utils.ToInt64(t)
		if ok {
			res, ok := intAdd(lhs, b)
			if ok {
				return res
			}
		}

	case float64:
		b_float, ok := utils.ToFloat(b)
		if ok {
			return t + b_float
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Add(scope, a, b)
		}
	}

	// Handle array concatenation
	if is_array(a) || is_array(b) {
		a_slice := convertToSlice(a)
		b_slice := convertToSlice(b)

		return append(a_slice, b_slice...)
	}

	scope.Trace("Protocol Add not found for %v (%T) and %v (%T)",
		a, a, b, b)
	return types.Null{}
}

func (self *AddDispatcher) AddImpl(elements ...AddProtocol) {
	for _, impl := range elements {
		self.impl = append([]AddProtocol{impl}, self.impl...)
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
