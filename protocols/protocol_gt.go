package protocols

import (
	"time"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Less than protocol
type GtProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Gt(scope types.Scope, a types.Any, b types.Any) bool
}

type GtDispatcher struct {
	impl []GtProtocol
}

func (self GtDispatcher) Copy() GtDispatcher {
	return GtDispatcher{
		append([]GtProtocol{}, self.impl...)}
}

func (self GtDispatcher) Gt(scope types.Scope, a types.Any, b types.Any) bool {
	switch t := a.(type) {
	case string:
		rhs, ok := b.(string)
		if ok {
			return t > rhs
		}

	case float64:
		rhs, ok := utils.ToFloat(b)
		if ok {
			return t > rhs
		}

	case time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.After(*rhs)
		}

	case *time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.After(*rhs)
		}
	}

	lhs, ok := utils.ToInt64(a)
	if ok {
		rhs, ok := utils.ToInt64(b)
		if ok {
			return lhs > rhs
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Gt(scope, a, b)
		}
	}

	return false
}

func (self *GtDispatcher) AddImpl(elements ...GtProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}
