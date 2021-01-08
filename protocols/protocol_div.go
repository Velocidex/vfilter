package protocols

import (
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Divtiply protocol
type DivProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Div(scope types.Scope, a types.Any, b types.Any) types.Any
}

type DivDispatcher struct {
	impl []DivProtocol
}

func (self DivDispatcher) Copy() DivDispatcher {
	return DivDispatcher{
		append([]DivProtocol{}, self.impl...)}
}

func (self DivDispatcher) Div(scope types.Scope, a types.Any, b types.Any) types.Any {
	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Div(scope, a, b)
		}
	}

	scope.Trace("Protocol Div not found for %v (%T) and %v (%T)",
		a, a, b, b)

	return types.Null{}
}

func (self *DivDispatcher) AddImpl(elements ...DivProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _NumericDiv struct{}

func (self _NumericDiv) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToFloat(a)
	_, b_ok := utils.ToFloat(b)
	return a_ok && b_ok
}

func (self _NumericDiv) Div(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToFloat(a)
	b_val, _ := utils.ToFloat(b)
	if b_val == 0 {
		return false
	}

	return a_val / b_val
}

type _DivInt struct{}

func (self _DivInt) Applicable(a types.Any, b types.Any) bool {
	return utils.IsInt(a) && utils.IsInt(b)
}

func (self _DivInt) Div(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToInt64(a)
	b_val, _ := utils.ToInt64(b)
	if b_val == 0 {
		return false
	}

	return a_val / b_val
}
