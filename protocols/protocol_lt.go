package protocols

import (
	"time"

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
	a = maybeReduce(a)
	b = maybeReduce(b)

	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return false

	case string:
		rhs, ok := b.(string)
		if ok {
			return t < rhs
		}

	case float64:
		rhs, ok := utils.ToFloat(b)
		if ok {
			return t < rhs
		}

	case time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.Before(*rhs)
		}

	case *time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.Before(*rhs)
		}
	}

	lhs, ok := utils.ToInt64(a)
	if ok {
		rhs, ok := utils.ToInt64(b)
		if ok {
			return lhs < rhs
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Lt(scope, a, b)
		}
	}

	return false
}

func toTime(a types.Any) (*time.Time, bool) {
	switch t := a.(type) {
	case time.Time:
		return &t, true
	case *time.Time:
		return t, true
	default:
		return nil, false
	}
}

func (self *LtDispatcher) AddImpl(elements ...LtProtocol) {
	for _, impl := range elements {
		self.impl = append([]LtProtocol{impl}, self.impl...)
	}
}
