package protocols

import (
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Multiply protocol
type MulProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Mul(scope types.Scope, a types.Any, b types.Any) types.Any
}

type MulDispatcher struct {
	impl []MulProtocol
}

func (self MulDispatcher) Copy() MulDispatcher {
	return MulDispatcher{
		append([]MulProtocol{}, self.impl...)}
}

func (self MulDispatcher) Mul(scope types.Scope, a types.Any, b types.Any) types.Any {
	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		b_float, ok := utils.ToFloat(b)
		if ok {
			return t * b_float
		}
	}

	switch t := b.(type) {
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		a_float, ok := utils.ToFloat(a)
		if ok {
			return t * a_float
		}
	}

	a_int, ok := utils.ToInt64(a)
	if ok {
		b_int, ok := utils.ToInt64(b)
		if ok {
			return a_int * b_int
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Mul(scope, a, b)
		}
	}
	scope.Trace("Protocol Mul not found for %v (%T) and %v (%T)",
		a, a, b, b)

	return types.Null{}
}

func (self *MulDispatcher) AddImpl(elements ...MulProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}
