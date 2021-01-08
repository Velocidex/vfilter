package protocols

import (
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Sub protocol
type SubProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Sub(scope types.Scope, a types.Any, b types.Any) types.Any
}

type SubDispatcher struct {
	impl []SubProtocol
}

func (self SubDispatcher) Copy() SubDispatcher {
	return SubDispatcher{
		append([]SubProtocol{}, self.impl...)}
}

func (self SubDispatcher) Sub(scope types.Scope, a types.Any, b types.Any) types.Any {
	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Sub(scope, a, b)
		}
	}

	scope.Trace("Protocol Sub not found for %v (%T) and %v (%T)",
		a, a, b, b)
	return types.Null{}
}

func (self *SubDispatcher) AddImpl(elements ...SubProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _SubFloats struct{}

func (self _SubFloats) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToFloat(a)
	_, b_ok := utils.ToFloat(b)
	return a_ok && b_ok
}

func (self _SubFloats) Sub(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToFloat(a)
	b_val, _ := utils.ToFloat(b)
	return a_val - b_val
}

type _SubInts struct{}

func (self _SubInts) Applicable(a types.Any, b types.Any) bool {
	return utils.IsInt(a) && utils.IsInt(b)
}

func (self _SubInts) Sub(scope types.Scope, a types.Any, b types.Any) types.Any {
	a_val, _ := utils.ToInt64(a)
	b_val, _ := utils.ToInt64(b)
	return a_val - b_val
}
