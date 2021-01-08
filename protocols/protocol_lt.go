package protocols

import (
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Less than protocol
type LtProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Lt(scope types.Scope, a types.Any, b types.Any) bool
}

type LtDispatcher struct {
	impl []LtProtocol
}

func (self LtDispatcher) Copy() LtDispatcher {
	return LtDispatcher{
		append([]LtProtocol{}, self.impl...)}
}

func (self LtDispatcher) Lt(scope types.Scope, a types.Any, b types.Any) bool {
	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Lt(scope, a, b)
		}
	}

	return false
}

func (self LtDispatcher) Applicable(scope types.Scope, a types.Any, b types.Any) bool {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return true
		}
	}

	scope.Trace("Protocol LessThan not found for %v (%T) and %v (%T)",
		a, a, b, b)
	return false
}

func (self *LtDispatcher) AddImpl(elements ...LtProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _StringLt struct{}

func (self _StringLt) Lt(scope types.Scope, a types.Any, b types.Any) bool {
	a_str, _ := utils.ToString(a)
	b_str, _ := utils.ToString(b)

	return a_str < b_str
}

func (self _StringLt) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToString(a)
	_, b_ok := utils.ToString(b)
	return a_ok && b_ok
}

type _NumericLt struct{}

func (self _NumericLt) Lt(scope types.Scope, a types.Any, b types.Any) bool {
	a_val, _ := utils.ToFloat(a)
	b_val, _ := utils.ToFloat(b)
	return a_val < b_val
}
func (self _NumericLt) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToFloat(a)
	_, b_ok := utils.ToFloat(b)
	return a_ok && b_ok
}
