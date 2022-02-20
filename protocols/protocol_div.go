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
	a = maybeReduce(a)

	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		b_float, ok := utils.ToFloat(b)
		if ok {
			if b_float == 0 {
				return &types.Null{}
			}
			return t / b_float
		}
	}

	switch t := b.(type) {
	case types.Null, *types.Null, nil:
		return &types.Null{}

	case float64:
		a_float, ok := utils.ToFloat(a)
		if ok {
			if a_float == 0 {
				return &types.Null{}
			}
			return t / a_float
		}
	}

	// Always convert to float to not lose preceision.
	a_int, ok := utils.ToInt64(a)
	if ok {
		b_int, ok := utils.ToFloat(b)
		if ok {
			if b_int == 0 {
				return &types.Null{}
			}
			return float64(a_int) / b_int
		}
	}

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
