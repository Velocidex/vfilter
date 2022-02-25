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
	a = maybeReduce(a)
	b = maybeReduce(b)

	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		b_float, ok := utils.ToFloat(b)
		if ok {
			return t - b_float
		}
	}

	switch t := b.(type) {
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		a_float, ok := utils.ToFloat(a)
		if ok {
			return t - a_float
		}
	}

	a_int, ok := utils.ToInt64(a)
	if ok {
		b_int, ok := utils.ToInt64(b)
		if ok {
			return a_int - b_int
		}
	}

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
