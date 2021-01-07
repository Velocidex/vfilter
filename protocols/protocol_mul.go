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

type _MulInt struct{}

func (self _MulInt) Applicable(a types.Any, b types.Any) bool {
	return utils.IsInt(a) && utils.IsInt(b)
}

func (self _MulInt) Mul(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToInt64(a)
	b_val, _ := utils.ToInt64(b)
	return a_val * b_val
}

type _NumericMul struct{}

func (self _NumericMul) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToFloat(a)
	_, b_ok := utils.ToFloat(b)
	return a_ok && b_ok
}

func (self _NumericMul) Mul(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToFloat(a)
	b_val, _ := utils.ToFloat(b)
	return a_val * b_val
}
